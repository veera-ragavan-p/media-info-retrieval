import json

from aws_lambda_powertools import Logger
from pymediainfo import MediaInfo

from remote_tech_validation.core.adapters.aws_adapter import AWSAdapter
from remote_tech_validation.core.exceptions.corrupted_file import CorruptedFile
from remote_tech_validation.core.exceptions.media_info_error import MediaInfoError


class MediaInfoAdapter:
    def __init__(
            self,
            aws_adapter: AWSAdapter,
            media_info=MediaInfo
    ):
        self._aws_adapter = aws_adapter
        self._media_info = media_info
        self._logger = Logger()

    def build_profile_from_mediainfo(self) -> dict:
        media_info = self._get_media_info()

        media_info_profile = {
            "video": {},
            "audio": {
                "format": []
            }
        }

        for track in media_info["tracks"]:
            if track["track_type"] == "General":
                media_info_profile['video']['container'] = track["format"]
                media_info_profile['video']['frameRate'] = get_framerate(track["frame_rate"])
                media_info_profile['video']["commercialName"] = track["commercial_name"]
            elif track["track_type"] == "Video":
                media_info_profile['video']['formatProfile'] = track["format_profile"]
                media_info_profile['video']['format'] = track["format"]
                media_info_profile['video']['width'] = str(track["width"])
                media_info_profile['video']['bitrate'] = get_bitrate(track["bit_rate"])
                media_info_profile['video']['scanType'] = track["scan_type"]
                media_info_profile['video']['height'] = str(track["sampled_height"])
                media_info_profile["video"]["colorPrimaries"] = track.get("color_primaries")
                media_info_profile["video"]["transferCharacteristics"] = track.get("transfer_characteristics")
            elif track["track_type"] == "Audio":
                media_info_profile["audio"]["format"].append(track["format"])

        self._logger.info('BUILT PROFILE')
        self._logger.info(str(media_info_profile))
        return media_info_profile

    def _get_media_info(self) -> dict:
        try:
            signed_url = self._aws_adapter.get_signed_url_for_asset()
            self._logger.debug("LAUNCHING MEDIA INFO")
            media_info_object = self._media_info.parse(
                signed_url,
                library_file="/opt/libmediainfo.so.0"
            )
            media_info = json.loads(media_info_object.to_json())
            self._logger.info('MEDIA INFO')
            self._logger.info(json.dumps(media_info))
            self._check_is_file_corrupted(media_info)
            return media_info
        except FileNotFoundError as e:
            raise e
        except CorruptedFile as e:
            raise e
        except Exception as e:
            self._logger.error(f"Error parsing media info: {str(e)}", exc_info=True)
            raise MediaInfoError(str(e))

    def _check_is_file_corrupted(self, media_info: dict):
        for track in media_info["tracks"]:
            if track["track_type"] == "General" and track.get("istruncated") == "Yes":
                raise CorruptedFile()


def get_framerate(framerate):
    return framerate.split(".", maxsplit=1)[0] if framerate.endswith(".000") else framerate


# return bitrate
def get_bitrate(inrate):
    return inrate / 1000000
