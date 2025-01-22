"""Custom client handling, including VkStream base class."""

from __future__ import annotations

import typing as t

from vk_api import VkApi


from singer_sdk.streams import Stream
from singer_sdk.helpers.jsonpath import extract_jsonpath

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context


class VkStream(Stream):
    """Stream class for Vk streams."""
    records_jsonpath = "$[*]"

    def get_records(
        self,
        context: Context | None,
    ) -> t.Iterable[dict]:
        """Return a generator of record-type dictionary objects.

        The optional `context` argument is used to identify a specific slice of the
        stream if partitioning is required for the stream. Most implementations do not
        require partitioning and should ignore the `context` argument.

        Args:
            context: Stream partition or context dictionary.

        Raises:
            NotImplementedError: If the implementation is TODO
        """
        # Ваш токен доступа
        params = self.config.get("params") or {}
        token = params.get('token')
        vk_session = VkApi(token=token)

        # Получаем объект VK_API
        vk = vk_session.get_api()

        records = vk.groups.get(user_id=params.get('user_id'), filter='admin')
        #for record in records:
         #   yield record.to_dict()
        yield from extract_jsonpath(self.records_jsonpath, input=records)
