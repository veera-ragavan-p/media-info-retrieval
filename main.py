from pymediainfo import MediaInfo
import os
from pprint import pprint


file_path = "/Users/vpb052/Downloads/Nature.mp4"
print(os.path.isfile(file_path))


media_info = MediaInfo.parse(file_path)
for track in media_info.tracks:
    if track.track_type == "Video":
        print(f"Bit rate: {track.bit_rate}, Frame rate: {track.frame_rate}, Format: {track.format}")
        print("Duration (raw value):", track.duration)
        print("Duration (other values:")
        pprint(track.other_duration)
    elif track.track_type == "Audio":
        print("Track data:")
        pprint(track.to_data()) 