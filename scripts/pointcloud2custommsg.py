#!/usr/bin/env python

"""Convert Livox PointCloud2 messages to CustomMsg records inside a rosbag."""

import argparse
import collections
import sys

import rosbag
from sensor_msgs.msg import PointCloud2
from sensor_msgs import point_cloud2

from livox_ros_driver2.msg import CustomMsg, CustomPoint

UINT8_MAX = 0xFF
UINT32_MAX = 0xFFFFFFFF

ConversionStats = collections.namedtuple(
    "ConversionStats", ["processed", "converted", "skipped_empty", "skipped_errors"]
)


def clamp_uint8(value):
    """Clamp a numeric value into the uint8 range."""
    return max(0, min(UINT8_MAX, int(round(value))))


def clamp_uint32(value):
    """Clamp a numeric value into the uint32 range."""
    return max(0, min(UINT32_MAX, int(round(value))))


def convert_cloud_to_custom(cloud, lidar_id):
    """Convert a Livox-formatted PointCloud2 message into a CustomMsg."""

    points = list(
        point_cloud2.read_points(
            cloud,
            field_names=("x", "y", "z", "intensity", "tag", "line", "timestamp"),
            skip_nans=False,
        )
    )

    if not points:
        return None

    msg = CustomMsg()
    msg.header = cloud.header
    msg.lidar_id = clamp_uint8(lidar_id)
    msg.rsvd = [0, 0, 0]

    # Determine timebase using the first point timestamp when available,
    # otherwise fall back to the header stamp.
    first_point_time = int(round(points[0][6]))
    header_time = int(msg.header.stamp.to_nsec()) if msg.header.stamp else 0
    timebase = first_point_time if first_point_time > 0 else header_time
    if timebase <= 0 and header_time > 0:
        timebase = header_time
    msg.timebase = timebase

    for x, y, z, intensity, tag, line, ts in points:
        custom_point = CustomPoint()
        custom_point.x = float(x)
        custom_point.y = float(y)
        custom_point.z = float(z)
        custom_point.reflectivity = clamp_uint8(intensity)
        custom_point.tag = clamp_uint8(tag)
        custom_point.line = clamp_uint8(line)
        offset = max(0, int(round(ts)) - timebase)
        custom_point.offset_time = clamp_uint32(offset)
        msg.points.append(custom_point)

    msg.point_num = len(msg.points)
    return msg


def process_bag(
    input_path,
    output_path,
    source_topic,
    target_topic,
    lidar_id,
):
    stats = {
        "processed": 0,
        "converted": 0,
        "skipped_empty": 0,
        "skipped_errors": 0,
    }

    with rosbag.Bag(output_path, "w") as out_bag:
        with rosbag.Bag(input_path, "r") as in_bag:
            for topic, msg, timestamp in in_bag.read_messages():
                if topic != source_topic:
                    out_bag.write(topic, msg, timestamp)
                    continue

                stats["processed"] += 1
                try:
                    converted = convert_cloud_to_custom(msg, lidar_id)
                except Exception:  # pragma: no cover - defensive logging only
                    stats["skipped_errors"] += 1
                    out_bag.write(topic, msg, timestamp)
                    continue

                if converted is None:
                    stats["skipped_empty"] += 1
                    out_bag.write(topic, msg, timestamp)
                    continue

                out_bag.write(target_topic, converted, timestamp)
                stats["converted"] += 1

    return ConversionStats(**stats)


def build_cli():
    parser = argparse.ArgumentParser(
        description=(
            "Convert Livox PointCloud2 messages inside a rosbag into "
            "livox_ros_driver2/CustomMsg messages."
        )
    )
    parser.add_argument("input", help="Path to the input rosbag (.bag) file")
    parser.add_argument("output", help="Destination rosbag that will receive CustomMsg records")
    parser.add_argument(
        "--source-topic",
        default="/livox/lidar",
        help="Topic in the input bag that carries sensor_msgs/PointCloud2 data (default: %(default)s)",
    )
    parser.add_argument(
        "--target-topic",
        default="/livox/lidar",
        help="Topic name used for the generated CustomMsg data (default: %(default)s)",
    )
    parser.add_argument(
        "--lidar-id",
        type=int,
        default=0,
        help="Numeric lidar identifier to embed into CustomMsg.lidar_id (default: %(default)s)",
    )
    return parser


def main(argv=None):
    parser = build_cli()
    args = parser.parse_args(argv)

    stats = process_bag(
        input_path=args.input,
        output_path=args.output,
        source_topic=args.source_topic,
        target_topic=args.target_topic,
        lidar_id=args.lidar_id,
    )

    sys.stdout.write(
        (
            "Processed {processed} PointCloud2 messages, converted {converted}, "
            "skipped {skipped_empty} empty and {skipped_errors} error messages.\n"
        ).format(
            processed=stats.processed,
            converted=stats.converted,
            skipped_empty=stats.skipped_empty,
            skipped_errors=stats.skipped_errors,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
