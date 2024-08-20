import os
import json
import shutil
import tarfile
import tempfile
import docker
import logging
import sys

DIFF_OUTPUT_DIR = "output-diff-images"
NEW_RELEASES_DIR = "new-releases"


def setup_logging():
    log = logging.getLogger()
    log.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    log.addHandler(handler)
    return log


def pull_image(client, image, tag, log):
    try:
        client.images.get(f"{image}:{tag}")
    except docker.errors.ImageNotFound:
        log.info(
            f"Docker image {image}:{tag} not found locally. Pulling from registry..."
        )
        client.images.pull(image, tag)


def extract_layers_and_files(client, image, tag, temp_dir, log):
    pull_image(client, image, tag, log)
    image_obj = client.images.get(f"{image}:{tag}")
    image_tar_path = os.path.join(temp_dir, "image.tar")
    with open(image_tar_path, "wb") as tar_file:
        for chunk in image_obj.save(named=True):
            tar_file.write(chunk)

    log.info(f"Extracting contents from {image_tar_path}")
    with tarfile.open(image_tar_path, "r") as tar:
        tar.extractall(os.path.join(temp_dir, "layers"))


def read_from_blobs(directory_path):
    sha256_dir = os.path.join(directory_path, "blobs", "sha256")
    if not os.path.isdir(sha256_dir):
        raise FileNotFoundError(
            f"The directory {sha256_dir} does not exist or is not a directory."
        )
    return {
        os.path.normpath(os.path.join("blobs", "sha256", f))
        for f in os.listdir(sha256_dir)
    }


def read_layers_from_manifest(manifest_json_path):
    with open(manifest_json_path, "r") as f:
        manifest = json.load(f)
    return {
        os.path.normpath(layer) for item in manifest for layer in item.get("Layers", [])
    }


def save_differences(old_files, new_files, output_file, log):
    diff = {
        "added": list(new_files - old_files),
        "removed": list(old_files - new_files),
    }
    with open(output_file, "w") as f:
        json.dump(diff, f, indent=4)
    log.info(f"Differences saved to {output_file}")


def generate_diff(client, image, tag1, tag2, diff_output_dir, log):
    reg_name_removed_img = image.replace("/", "_").replace("\\", "_")
    log.info(f"Processing image {image} with tags {tag1} and {tag2}")

    temp_dir_old_ver = tempfile.mkdtemp()
    temp_dir_r2 = tempfile.mkdtemp()
    temp_dir_diff = tempfile.mkdtemp()

    os.makedirs(os.path.join(temp_dir_diff, "blobs", "sha256"), exist_ok=True)

    try:
        extract_layers_and_files(client, image, tag1, temp_dir_old_ver, log)
        extract_layers_and_files(client, image, tag2, temp_dir_r2, log)

        old_version_layers = read_from_blobs(os.path.join(temp_dir_old_ver, "layers"))
        new_version_layers = read_from_blobs(os.path.join(temp_dir_r2, "layers"))

        log.info(f"Comparing layers between {image}:{tag1} and {image}:{tag2}")

        for curr_layer in new_version_layers:
            if curr_layer not in old_version_layers:
                log.info(
                    f"[INFO] Layer {curr_layer} is new or changed in {image}:{tag2}"
                )
                src = os.path.join(temp_dir_r2, "layers", curr_layer)
                dst = os.path.join(temp_dir_diff, "blobs", "sha256")
                shutil.copy(src, dst)

        shutil.copy(os.path.join(temp_dir_r2, "layers", "manifest.json"), temp_dir_diff)
        shutil.copy(os.path.join(temp_dir_r2, "layers", "repositories"), temp_dir_diff)

        diff_file = os.path.join(temp_dir_diff, f"diff_{tag2}.json")
        save_differences(old_version_layers, new_version_layers, diff_file, log)

        diff_tar = os.path.join(
            diff_output_dir, f"{reg_name_removed_img}_diff_{tag2}.tar"
        )
        log.info(f"Creating diff tar file {diff_tar}")
        os.makedirs(diff_output_dir, exist_ok=True)
        with tarfile.open(diff_tar, "w") as tar:
            tar.add(temp_dir_diff, arcname="")

        log.info(f"Diff tar created successfully: {diff_tar}")

    finally:
        shutil.rmtree(temp_dir_old_ver)
        shutil.rmtree(temp_dir_r2)
        shutil.rmtree(temp_dir_diff)


def process_image(client, image, tag1, tag2, diff_output_dir, new_releases_dir, log):
    updated_image = image.replace("/", "_").replace("\\", "_")
    diff_tar = os.path.join(diff_output_dir, f"{updated_image}_diff_{tag2}.tar")

    if not os.path.exists(diff_tar):
        log.error(f"Diff tar file {diff_tar} does not exist")
        return

    temp_dir_old_ver = tempfile.mkdtemp()
    temp_dir_diff = tempfile.mkdtemp()
    temp_dir_new_ver = tempfile.mkdtemp()

    os.makedirs(os.path.join(temp_dir_new_ver, "blobs", "sha256"), exist_ok=True)

    try:
        log.info(
            f"[INFO] Saving docker outdated image {image}:{tag1} to {os.path.join(temp_dir_old_ver, 'image_r1.tar')}"
        )
        pull_image(client, image, tag1, log)
        image_obj = client.images.get(f"{image}:{tag1}")
        image_r1_tar = os.path.join(temp_dir_old_ver, "image_r1.tar")
        with open(image_r1_tar, "wb") as tar_file:
            for chunk in image_obj.save():
                tar_file.write(chunk)

        log.info(
            f"[INFO] Extracting contents from {os.path.join(temp_dir_old_ver, 'image_r1.tar')}"
        )

        with tarfile.open(image_r1_tar, "r") as tar:
            tar.extractall(temp_dir_old_ver)
        os.remove(image_r1_tar)

        with tarfile.open(diff_tar, "r") as tar:
            tar.extractall(temp_dir_diff)

        log.info(f"[INFO] Extracting contents from {diff_tar} to {temp_dir_diff}")
        diff_json = os.path.join(temp_dir_diff, f"diff_{tag2}.json")
        if not os.path.exists(diff_json):
            log.error(f"{diff_json} not found in {diff_tar}")
            return

        with open(diff_json) as f:
            layers_add = json.load(f)["added"]

        log.info(f"[INFO] Preparing updated diff")
        os.makedirs(temp_dir_new_ver, exist_ok=True)

        extracted_dirs = sorted(
            [
                os.path.join("blobs", "sha256", file_name)
                for file_name in os.listdir(
                    os.path.join(temp_dir_diff, "blobs", "sha256")
                )
            ]
        )

        # Copy different layers to the updated diff tar
        for dir_name in extracted_dirs:
            src_dir = os.path.join(temp_dir_diff, dir_name)
            dest_dir = os.path.join(temp_dir_new_ver, dir_name)
            if os.path.exists(src_dir):
                shutil.copy(src_dir, dest_dir)

        log.info(f"[INFO] Different layers were copied to the updated diff tar")

        # Copy remaining manifest and repository files to diff tar
        remaining_files = [
            os.path.join(temp_dir_old_ver, file_name)
            for file_name in os.listdir(temp_dir_old_ver)
        ]

        for dir_name in remaining_files:
            if os.path.isdir(dir_name):
                continue
            src_dir = os.path.join(temp_dir_diff, dir_name)
            dest_dir = os.path.join(
                temp_dir_new_ver, os.path.relpath(dir_name, temp_dir_old_ver)
            )
            if os.path.exists(dest_dir):
                continue
            if os.path.exists(src_dir):
                shutil.copy(src_dir, dest_dir)

        shutil.copy(os.path.join(temp_dir_diff, "manifest.json"), temp_dir_new_ver)

        old_version_layers = read_layers_from_manifest(
            os.path.join(temp_dir_old_ver, "manifest.json")
        )

        log.info(f"[INFO] Checking for missing layers...")

        # Check for layers that are missing in the diff tar
        for layer in layers_add:
            if layer not in extracted_dirs:
                log.info(
                    f"[INFO] Layer {layer} is missing in the diff tar. Copying from old version"
                )
                if layer in old_version_layers:
                    src = os.path.join(temp_dir_old_ver, layer)
                    dst = os.path.join(temp_dir_new_ver, layer)
                    shutil.copy(src, dst)
                else:
                    log.error(
                        f"[ERROR] Layer {layer} is missing in the old version. Cannot proceed."
                    )
                    return

        log.info(f"[INFO] Checking for removed layers...")

        os.makedirs("new-releases", exist_ok=True)
        updated_diff_tar = os.path.join(
            new_releases_dir, f"{updated_image}_diff_{tag2}.tar"
        )
        log.info(f"[SUCCESS] Creating updated diff tar file {updated_diff_tar}")
        with tarfile.open(updated_diff_tar, "w") as tar:
            tar.add(temp_dir_new_ver, arcname="")

        # Load the docker image
        log.info(f"[INFO] Loading updated image into the target environment repository")
        client.images.load(open(updated_diff_tar, "rb"))

        log.info(f"[SUCCESS] Image loaded into the target environment repository")

    finally:
        shutil.rmtree(temp_dir_old_ver)
        shutil.rmtree(temp_dir_diff)
        shutil.rmtree(temp_dir_new_ver)
