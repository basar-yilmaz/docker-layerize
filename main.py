import json
import sys
import docker
import os
import argparse
from utils import setup_logging, generate_diff, process_image

def parse_arguments():
    parser = argparse.ArgumentParser(description="Docker Image Layer Diff Tool")
    parser.add_argument("--dev", action="store_true", help="Generate diff tar files between old and new image layers")
    parser.add_argument("--process", action="store_true", help="Process the diff tar and create a final release image tar")
    parser.add_argument("--image-list", default="image_list.json", help="A JSON file containing the list of images and tags to process")
    parser.add_argument("--output-dir", type=str, default="output-diff-images", help="Directory to store the diff tar files")
    parser.add_argument("--release-dir", type=str, default="new-releases", help="Directory to store the final release image tar files")
    return parser.parse_args()

def main():
    args = parse_arguments()
    log = setup_logging()
    client = docker.from_env()

    image_list_json = args.image_list
    if not os.path.isfile(image_list_json):
        log.error(f"[ERROR] File {image_list_json} does not exist")
        sys.exit(1)

    with open(image_list_json) as f:
        image_list = json.load(f)

    for entry in image_list:
        image = entry.get("image")
        tag1 = entry.get("old_ver")
        tag2 = entry.get("new_ver")

        if not image or not tag1 or not tag2:
            log.error(f"[ERROR] Missing image or tags in entry: {entry}")
            continue

        log.info(f"Image to be processed: {image} from {tag1} to {tag2}.")

        if args.dev:
            generate_diff(client, image, tag1, tag2, args.output_dir, log)

        if args.process:
            process_image(client, image, tag1, tag2, args.output_dir, args.release_dir, log)

if __name__ == "__main__":
    main()
