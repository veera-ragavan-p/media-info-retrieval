from aws_lambda_powertools import Logger


class ProfileMatcher:
    def __init__(self):
        self._logger = Logger()

    def is_media_matching_with_any_profiles(self, content_profiles_list: list, media_profile: dict) -> bool:
        for profile in content_profiles_list:
            self._logger.info(f"comparing to {profile['contentProfileId']} profile")
            if self._does_media_match_with_profile(profile, media_profile):
                self._logger.info('matched profile ' + profile['contentProfileId'] + ' to mediaInfo output')
                return True

        return False

    def _does_media_match_with_profile(self, profile, media_profile) -> bool:
        return (self._does_media_match_with_video_profile(profile, media_profile)
                and self._does_media_match_with_audio_profile(profile, media_profile))

    def _does_media_match_with_video_profile(self, profile, media_profile) -> bool:
        for video_key, expected_value in profile.get("video", {}).items():
            self._logger.debug(f"checking for video.{video_key} value")
            actual_value = media_profile["video"][video_key]
            if actual_value != expected_value:
                self._logger.info(f"media video.{video_key} value '{actual_value}' does not match with profile value '"
                                  f"{expected_value}'")
                return False

        return True

    def _does_media_match_with_audio_profile(self, profile, media_profile) -> bool:
        # hardcoded for now, should make it dynamic after we have the new audio layout for SS API
        if audio_format := profile.get("audio", {}).get("format"):
            audio_format_list = media_profile["audio"]["format"]
            if audio_format not in audio_format_list:
                self._logger.info(
                    f"media audio.format '{audio_format_list}' does not have profile value '{audio_format}'")
                return False

        return True
