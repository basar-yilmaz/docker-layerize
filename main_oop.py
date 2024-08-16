import json
import os
import shutil
import tarfile
import docker
import tempfile
import logging
import sys
import argparse

class DockerImageProcessor:
    DIFF_OUTPUT_DIR = "output-diff-images"
    NEW_RELEASES_DIR = "new-releases"

    def __init__(self, client):
        self.client = client
        self.log = logging.getLogger()

    def pull_image(self, image, tag):
        try:
            self.client.images.get(f"{image}:{tag}")
        except docker.errors.ImageNotFound:
            self.log.info(f"Docker image {image}:{tag} not found locally. Pulling from registry...")
            self.client.images.pull(image, tag)

    def extract_layers_and_files(self, image, tag, temp_dir):
        self.pull_image(image, tag)
        image_obj = self.client.images.get(f"{image}:{tag}")
        with open(os.path.join(temp_dir, "image.tar"), "wb") as tar_file:
            for chunk in image_obj.save(named=True):
                tar_file.write(chunk)

        self.log.info(f"Extracting contents from {os.path.join(temp_dir, 'image.tar')}")
        with tarfile.open(os.path.join(temp_dir, "image.tar"), "r") as tar:
            tar.extractall(os.path.join(temp_dir, "layers"))

    def read_from_blobs(self, directory_path):
        sha256_dir = os.path.join(directory_path, "blobs", "sha256")
        layer_files = set()

        if os.path.isdir(sha256_dir):
            for file_name in os.listdir(sha256_dir):
                file_path = os.path.join(sha256_dir, file_name)
                if os.path.isfile(file_path):
                    relative_path = os.path.join("blobs", "sha256", file_name)
                    layer_files.add(os.path.normpath(relative_path))
        else:
            raise FileNotFoundError(f"The directory {sha256_dir} does not exist or is not a directory.")

        return layer_files

    def read_layers_from_manifest(self, manifest_json_path):
        with open(manifest_json_path, "r") as f:
            manifest = json.load(f)

        layer_dirs = set()
        for item in manifest:
            layers = item.get("Layers", [])
            for layer in layers:
                layer_dirs.add(os.path.normpath(layer))  
        return layer_dirs

    def save_differences(self, old_files, new_files, output_file):
        diff = {
            "added": list(new_files - old_files),
            "removed": list(old_files - new_files),
        }
        with open(output_file, "w") as f:
            json.dump(diff, f, indent=4)
        self.log.info(f"Differences saved to {output_file}")

    def generate_diff(self, image, tag1, tag2):
        reg_name_removed_img = image.replace("/", "_").replace("\\", "_")
        self.log.info(f"Processing image {image} with tags {tag1} and {tag2}")

        temp_dir_r1 = tempfile.mkdtemp()
        temp_dir_r2 = tempfile.mkdtemp()
        temp_dir_diff = tempfile.mkdtemp()

        os.makedirs(os.path.join(temp_dir_diff, "blobs", "sha256"), exist_ok=True)

        try:
            self.extract_layers_and_files(image, tag1, temp_dir_r1)
            self.extract_layers_and_files(image, tag2, temp_dir_r2)

            old_version_layers = self.read_from_blobs(os.path.join(temp_dir_r1, "layers"))
            new_version_layers = self.read_from_blobs(os.path.join(temp_dir_r2, "layers"))

            self.log.info(f"Comparing layers between {image}:{tag1} and {image}:{tag2}")

            for curr_layer in new_version_layers:
                if curr_layer not in old_version_layers:
                    self.log.info(f"Layer {curr_layer} is new or changed in {image}:{tag2}")
                    src = os.path.join(temp_dir_r2, "layers", curr_layer)
                    dst = os.path.join(temp_dir_diff, "blobs", "sha256")
                    shutil.copy(src, dst)

            shutil.copy(os.path.join(temp_dir_r2, "layers", "manifest.json"), temp_dir_diff)
            shutil.copy(os.path.join(temp_dir_r2, "layers", "repositories"), temp_dir_diff)

            diff_file = os.path.join(temp_dir_diff, f"diff_{tag2}.json")
            self.save_differences(old_version_layers, new_version_layers, diff_file)

            diff_tar = os.path.join(self.DIFF_OUTPUT_DIR, f"{reg_name_removed_img}_diff_{tag2}.tar")

            self.log.info(f"Creating diff tar file {diff_tar}")
            with tarfile.open(diff_tar, "w") as tar:
                tar.add(temp_dir_diff, arcname="")

            self.log.info(f"Diff tar created successfully: {diff_tar}")

        finally:
            shutil.rmtree(temp_dir_r1)
            shutil.rmtree(temp_dir_r2)
            shutil.rmtree(temp_dir_diff)

    def process_image(self, image, tag1, tag2):
        updated_image = image.replace("/", "_").replace("\\", "_")
        diff_tar = os.path.join(self.DIFF_OUTPUT_DIR, f"{updated_image}_diff_{tag2}.tar")

        if not os.path.exists(diff_tar):
            self.log.error(f"Diff tar file {diff_tar} does not exist")
            return

        temp_dir_r1 = tempfile.mkdtemp()
        temp_dir_diff = tempfile.mkdtemp()
        temp_dir_updated_diff = tempfile.mkdtemp()

        os.makedirs(os.path.join(temp_dir_updated_diff, "blobs", "sha256"), exist_ok=True)

        try:
            self.pull_image(image, tag1)
            image_obj = self.client.images.get(f"{image}:{tag1}")
            with open(os.path.join(temp_dir_r1, "image_r1.tar"), "wb") as tar_file:
                for chunk in image_obj.save():
                    tar_file.write(chunk)

            with tarfile.open(os.path.join(temp_dir_r1, "image_r1.tar"), "r") as tar:
                tar.extractall(temp_dir_r1)

            os.remove(os.path.join(temp_dir_r1, "image_r1.tar"))

            with tarfile.open(diff_tar, "r") as tar:
                tar.extractall(temp_dir_diff)

            diff_json = os.path.join(temp_dir_diff, f"diff_{tag2}.json")
            if not os.path.exists(diff_json):
                self.log.error(f"{diff_json} not found in {diff_tar}")
                return

            with open(diff_json) as f:
                layers_add = json.load(f)["added"]

            self.log.info(f"Preparing updated diff")

            extracted_dirs = sorted(
                [
                    os.path.join("blobs", "sha256", file_name)
                    for file_name in os.listdir(os.path.join(temp_dir_diff, "blobs", "sha256"))
                ]
            )

            for dir_name in extracted_dirs:
                src_dir = os.path.join(temp_dir_diff, dir_name)
                dest_dir = os.path.join(temp_dir_updated_diff, dir_name)
                if os.path.exists(src_dir):
                    shutil.copy(src_dir, dest_dir)

            remaining_files = [
                os.path.join(temp_dir_r1, file_name)
                for file_name in os.listdir(temp_dir_r1)
            ]

            for dir_name in remaining_files:
                if os.path.isdir(dir_name):
                    continue
                src_dir = os.path.join(temp_dir_diff, dir_name)
                dest_dir = os.path.join(temp_dir_updated_diff, os.path.relpath(dir_name, temp_dir_r1))
                if os.path.exists(dest_dir):
                    continue
                if os.path.exists(src_dir):
                    shutil.copy(src_dir, dest_dir)

            shutil.copy(os.path.join(temp_dir_diff, "manifest.json"), temp_dir_updated_diff)

            old_version_layers = self.read_layers_from_manifest(
                os.path.join(temp_dir_r1, "manifest.json")
            )

            self.log.info(f"Checking for missing layers...")

            for layer in layers_add:
                if layer not in extracted_dirs:
                    self.log.info(f"Layer {layer} is missing in the diff tar. Copying from old version")
                    if layer in old_version_layers:
                        src = os.path.join(temp_dir_r1, layer)
                        dst = os.path.join(temp_dir_updated_diff, layer)
                        shutil.copy(src, dst)
                    else:
                        self.log.error(f"Layer {layer} is missing in the old version. Cannot proceed.")
                        return

            updated_diff_tar = os.path.join(self.NEW_RELEASES_DIR, f"{updated_image}_diff_{tag2}.tar")
            self.log.info(f"Creating updated diff tar file {updated_diff_tar}")
            with tarfile.open(updated_diff_tar, "w") as tar:
                tar.add(temp_dir_updated_diff, arcname="")

            self.log.info(f"Updated diff tar created successfully: {updated_diff_tar}")

        finally:
            shutil.rmtree(temp_dir_r1)
            shutil.rmtree(temp_dir_diff)
            shutil.rmtree(temp_dir_updated_diff)


def setup_logging():
    log = logging.getLogger()
    log.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    log.addHandler(handler)
    return log


def parse_arguments():
    parser = argparse.ArgumentParser(description="Docker Image Layer Diff Tool")
    parser.add_argument(
        "--dev",
        action="store_true",
        help="Generate diff tar files between old and new image layers"
    )
    parser.add_argument(
        "--process",
        action="store_true",
        help="Process the diff tar and create a final release image tar"
    )
    parser.add_argument(
        "--image-list",
        default="image_list.json",
        help="A json file containing the list of images and tags to process"
    )
    # parser.add_argument(
    #     "--image",
    #     type=str,
    #     required=True,
    #     help="The name of the Docker image"
    # )
    # parser.add_argument(
    #     "--tag1",
    #     type=str,
    #     required=True,
    #     help="The tag for the old version of the Docker image"
    # )
    # parser.add_argument(
    #     "--tag2",
    #     type=str,
    #     required=True,
    #     help="The tag for the new version of the Docker image"
    # )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="output-diff-images",
        help="Directory to store the diff tar files"
    )
    parser.add_argument(
        "--release-dir",
        type=str,
        default="new-releases",
        help="Directory to store the final release image tar files"
    )
    return parser.parse_args()


def main():
    args = parse_arguments()
    log = setup_logging()
    client = docker.from_env()

    processor = DockerImageProcessor(client)

    DockerImageProcessor.DIFF_OUTPUT_DIR = args.output_dir
    DockerImageProcessor.NEW_RELEASES_DIR = args.release_dir
    
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
            processor.generate_diff(args.image, args.tag1, args.tag2)

        if args.process:
            processor.process_image(args.image, args.tag1, args.tag2)


if __name__ == "__main__":
    main()
