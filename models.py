import os
from enum import IntEnum

from tortoise import fields, models
from tortoise.exceptions import ValidationError
from tortoise.validators import Validator

class Codec(IntEnum):
    MP3 = 1
    AAC = 2


class Station(models.Model):
    id = fields.IntField(pk=True)
    station_identifier = fields.CharField(max_length=120)
    url = fields.CharField(max_length=1024)
    codec = fields.IntEnumField(Codec)

    class Meta:
        table = "Radio stations"


class ValidateExtension(Validator):
    def __init__(self, allowed_extensions: list):
        self.allowed_extensions = allowed_extensions

    def __call__(self, value: str):
        ext = os.path.splitext(value)[1].lower()
        if ext not in self.allowed_extensions:
            raise ValidationError(
                f"Unsupported file extension. Allowed: {self.allowed_extensions}"
            )


class StreamClip(models.Model):
    id = fields.IntField(pk=True)
    station_id = fields.ForeignKeyField(
        "models.Station", related_name="station_id", on_delete=fields.CASCADE
    )

    # where the clip is stored.
    file_path = fields.CharField(
        max_length=1024, validators=[ValidateExtension([".mp3", ".aac"])]
    )

    # raw stream title
    raw_stream_title = fields.CharField(max_length=2048, null=True)

    # parsed artist name and title
    parser_metadata = fields.ForeignKeyField(
        "models.StreamTitleParserMetadata",
        related_name="parsed_meta",
        on_delete=fields.SET_NULL,
        null=True
    )
    artists = fields.CharField(max_length=1024, null=True)
    title = fields.CharField(max_length=1024, null=True)
    is_track = fields.BooleanField(default=False)


class TitleParser(IntEnum):
    REGEX_PARSER = 1


class StreamTitleParserMetadata(models.Model):
    id = fields.IntField(pk=True)
    parser_type = fields.IntEnumField(TitleParser, default=TitleParser.REGEX_PARSER)
    metadata_fields = fields.JSONField()
