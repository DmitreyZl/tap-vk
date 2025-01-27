"""Stream type classes for tap-vk."""

from __future__ import annotations
from vk_api import VkApi
from vk_api.exceptions import ApiError
import typing as t
from importlib import resources
import datetime

from singer_sdk import typing as th  # JSON Schema typing helpers
from singer_sdk.helpers.jsonpath import extract_jsonpath
from tap_vk.client import VkStream

# TODO: Delete this is if not using json files for schema definition
SCHEMAS_DIR = resources.files(__package__) / "schemas"


# TODO: - Override `UsersStream` and `GroupsStream` with your own stream definition.
#       - Copy-paste as many times as needed to create multiple stream types.


class GroupPostsStream(VkStream):
    """Define custom stream."""

    name = "group_posts"
    primary_keys = ["post_id", "group_id"]
    cont = []

    schema = th.PropertiesList(
        th.Property(
            "post_id",
            th.IntegerType,
            description="The post's system ID",
        ),
        th.Property(
            "group_id",
            th.IntegerType,
            description="The group's system ID",
        ),
        th.Property("inner_type", th.StringType),
        th.Property("type", th.StringType),
        th.Property("text", th.StringType),
        th.Property("comments", th.IntegerType),
        th.Property("likes", th.IntegerType),
        th.Property("hide", th.IntegerType),
        th.Property("join_group", th.IntegerType),
        th.Property("links", th.IntegerType),
        th.Property("reach_subscribers", th.IntegerType),
        th.Property("reach_total", th.IntegerType),
        th.Property("reach_viral", th.IntegerType),
        th.Property("reach_ads", th.IntegerType),
        th.Property("report", th.IntegerType),
        th.Property("to_group", th.IntegerType),
        th.Property("unsubscribe", th.IntegerType)
    ).to_dict()

    def get_records(
            self,
            context: Context | None,
    ) -> t.Iterable[dict]:
        # Ваш токен доступа
        params = self.config.get("params") or {}
        token = self.config.get('token')
        vk_session = VkApi(token=token)

        # Получаем объект VK_API
        vk = vk_session.get_api()
        owner_id = 0 - int(self.config.get('group_id'))
        wall_posts = vk.wall.get(owner_id=owner_id, count=100)
        stat = []
        for i in wall_posts['items']:
            try:
                wall = vk.stats.getPostReach(owner_id=owner_id, post_ids=i['id'])
                w = wall[0]
            except ApiError:
                w = {"post_id": i['id'],
                     "hide": 0,
                     "join_group": 0,
                     "links": 0,
                     "reach_subscribers": 0,
                     "reach_total": 0,
                     "reach_viral": 0,
                     "reach_ads": 0,
                     "report": 0,
                     "to_group": 0,
                     "unsubscribe": 0
                     }
            n = {'group_id': i['owner_id'],
                 'inner_type': i['inner_type'],
                 'type': i['type'],
                 'text': i['text'],
                 'comments': i['comments']['count'],
                 'likes': i['likes']['count']}
            merged_dictionary = {**w, **n}
            stat.append(merged_dictionary)
        #   yield record.to_dict()
        yield from extract_jsonpath(self.records_jsonpath, input=stat)


class GroupsStream(VkStream):
    """Define custom stream."""

    name = "group"
    primary_keys = ["id"]

    schema = th.PropertiesList(
        th.Property(
            "id",
            th.IntegerType,
            description="The group's system ID",
        ),
        th.Property("name", th.StringType),
        th.Property("screen_name", th.StringType),
        th.Property("site", th.StringType),
        th.Property("photo_50", th.StringType),
        th.Property("photo_100", th.StringType),
        th.Property("photo_200", th.StringType),
        th.Property("is_closed", th.IntegerType),
        th.Property("is_admin", th.IntegerType),
        th.Property("admin_level", th.IntegerType),
        th.Property("is_member", th.IntegerType),
        th.Property("is_advertiser", th.IntegerType),
    ).to_dict()

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
        token = self.config.get('token')
        vk_session = VkApi(token=token)

        # Получаем объект VK_API
        vk = vk_session.get_api()

        stat = vk.groups.getById(group_id=self.config.get('group_id'), fields='members_count', extended=1)

        #   yield record.to_dict()
        yield from extract_jsonpath(self.records_jsonpath, input=stat)


class GroupStatStream(VkStream):
    """Define custom stream."""

    name = "groupStat"
    primary_keys = ["id", "period_from", "dimension", "source"]
    replication_key = "period_from"
    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("period_from", th.IntegerType),
        th.Property("period_to", th.IntegerType),
        th.Property("activity_comments", th.IntegerType),
        th.Property("activity_copies", th.IntegerType),
        th.Property("activity_hidden", th.IntegerType),
        th.Property("activity_likes", th.IntegerType),
        th.Property("activity_subscribed", th.IntegerType),
        th.Property("activity_unsubscribed", th.IntegerType),
        th.Property("mobile_reach", th.IntegerType),
        th.Property("total_reach", th.IntegerType),
        th.Property("reach_subscribers", th.IntegerType),
        th.Property("mobile_views", th.IntegerType),
        th.Property("total_views", th.IntegerType),
        th.Property("total_visitors", th.IntegerType),
        th.Property("dimension", th.StringType),
        th.Property("dimension_value", th.StringType),
        th.Property("count", th.IntegerType),
        th.Property("source", th.StringType),
    ).to_dict()

    @staticmethod
    def transform_to_flat_structure(data):
        rows = []

        for record in data:
            # Общие поля (1 уровень вложенности)
            if record.get('activity'):
                base_record = {
                    'period_from': record['period_from'],
                    'period_to': record['period_to'],
                    **{f'activity_{k}': v for k, v in record.get('activity').items()},
                    'mobile_reach': record['reach']['mobile_reach'],
                    'total_reach': record['reach']['reach'],
                    'reach_subscribers': record['reach']['reach_subscribers'],
                    'mobile_views': record['visitors']['mobile_views'],
                    'total_views': record['visitors']['views'],
                    'total_visitors': record['visitors']['visitors'],
                }
            else:
                base_record = {
                    'period_from': record['period_from'],
                    'period_to': record['period_to'],
                    'mobile_reach': record['reach']['mobile_reach'],
                    'total_reach': record['reach']['reach'],
                    'reach_subscribers': record['reach']['reach_subscribers'],
                    'mobile_views': record['visitors']['mobile_views'],
                    'total_views': record['visitors']['views'],
                    'total_visitors': record['visitors']['visitors'],
                }

            # Вложенность 2 уровня -> новые строки
            # Reach -> Age
            for age_group in record.get('reach', {}).get('age', []):
                row = base_record.copy()
                row.update({
                    'dimension': 'age',
                    'dimension_value': age_group['value'],
                    'count': age_group['count'],
                    'source': 'reach',
                })
                rows.append(row)

            # Reach -> Cities
            for city in record.get('reach', {}).get('cities', []):
                row = base_record.copy()
                row.update({
                    'dimension': 'city',
                    'dimension_value': city['name'],
                    'count': city['count'],
                    'source': 'reach',
                })
                rows.append(row)

            # Reach -> Countries
            for country in record.get('reach', {}).get('countries', []):
                row = base_record.copy()
                row.update({
                    'dimension': 'country',
                    'dimension_value': country['name'],
                    'count': country['count'],
                    'source': 'reach',
                })
                rows.append(row)

            # Reach -> Sex
            for sex_group in record.get('reach', {}).get('sex', []):
                row = base_record.copy()
                row.update({
                    'dimension': 'sex',
                    'dimension_value': sex_group['value'],
                    'count': sex_group['count'],
                    'source': 'reach',
                })
                rows.append(row)

            # Visitors -> Age
            for age_group in record.get('visitors', {}).get('age', []):
                row = base_record.copy()
                row.update({
                    'dimension': 'age',
                    'dimension_value': age_group['value'],
                    'count': age_group['count'],
                    'source': 'visitors',
                })
                rows.append(row)

            # Visitors -> Cities
            for city in record.get('visitors', {}).get('cities', []):
                row = base_record.copy()
                row.update({
                    'dimension': 'city',
                    'dimension_value': city['name'],
                    'count': city['count'],
                    'source': 'visitors',
                })
                rows.append(row)

            # Visitors -> Countries
            for country in record.get('visitors', {}).get('countries', []):
                row = base_record.copy()
                row.update({
                    'dimension': 'country',
                    'dimension_value': country['name'],
                    'count': country['count'],
                    'source': 'visitors',
                })
                rows.append(row)

            # Visitors -> Sex
            for sex_group in record.get('visitors', {}).get('sex', []):
                row = base_record.copy()
                row.update({
                    'dimension': 'sex',
                    'dimension_value': sex_group['value'],
                    'count': sex_group['count'],
                    'source': 'visitors',
                })
                rows.append(row)

        return rows

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
        token = self.config.get('token')
        vk_session = VkApi(token=token)

        # Получаем объект VK_API
        vk = vk_session.get_api()
        today = datetime.datetime.today()
        one_day = datetime.timedelta(days=22)
        yesterday = today - one_day
        time1 = datetime.time(0)  # 00:00
        date_time_1 = datetime.datetime.combine(yesterday, time1)
        timestamp_from = date_time_1.timestamp()
        stat = vk.stats.get(group_id=self.config.get('group_id'), app_id=self.config.get('app_id'),
                            timestamp_from=int(timestamp_from), interval='day', extended=1)
        stat = self.transform_to_flat_structure(stat)
        for record in stat:
            record.update({'id': self.config.get('group_id')})

        #   yield record.to_dict()
        yield from extract_jsonpath(self.records_jsonpath, input=stat)
