import json
import os
import shutil
import tarfile
import docker
import tempfile
import logging
import sys

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger()

DIFF_OUTPUT_DIR = "output-diff-images"
NEW_RELEASES_DIR = "new-releases"


def usage():
    print(
        """
    Usage: main.py [--dev | --target] <image-list.json>

    Options:
    --dev     Generate diff tar files between old and new image releases.
    --target  Process existing diff tar files and update them based on the new release. DIFF_OUTPUT_DIR will not change.
                Generated new releases images.tar can be viewed under NEW_RELEASES_DIR directory.
    --help    Show this help message.
    
    Arguments:
    <image-list.json> JSON file containing the list of images and their versions. The JSON should be in the following format:
                        [
                        {
                            "Image": "myimage",
                            "Old Version": "r1",
                            "New Version": "r2"
                        },
                        { 
                            "Image": "registry/image",
                            "Old Version": "0.61.0",
                            "New Version": "0.63.0"
                        }
                        ]
    Examples:
    main.py --dev json-reg-images.json
    main.py --target image-list.json
    """
    )
    sys.exit(1)


def pull_image(client, image, tag):
    """
    Pull the Docker image from the registry if it does not exist locally.
    
    :param client: Docker client object.
    :param image: Name of the Docker image.
    :param tag: Tag of the Docker image
    """
    try:
        client.images.get(f"{image}:{tag}")
    except docker.errors.ImageNotFound:
        log.info(
            f"[INFO] Docker image {image}:{tag} not found locally. Pulling from registry..."
        )
        client.images.pull(image, tag)


def read_layers_from_manifest(manifest_json_path):
    """
    Read the layers from the manifest JSON file.

    :param manifest_json_path: Path to the manifest.json file.
    :return: List of layer directories.
    """
    with open(manifest_json_path, "r") as f:
        manifest = json.load(f)

    # Extract layer directories from manifest JSON
    layer_dirs = set()
    for item in manifest:
        layers = item.get("Layers", [])
        for layer in layers:
            layer_dirs.add(os.path.normpath(layer))  # Normalize path
    return layer_dirs


def read_from_blobs(directory_path):
    """
    Read the SHA256 layer files from the blobs/sha256 directory.

    :param directory_path: Path to the directory containing the blobs/sha256 subdirectory.
    :return: Set of relative paths for SHA256 layer files.
    """
    sha256_dir = os.path.join(directory_path, "blobs", "sha256")
    layer_files = set()

    if os.path.isdir(sha256_dir):
        for file_name in os.listdir(sha256_dir):
            file_path = os.path.join(sha256_dir, file_name)
            if os.path.isfile(file_path):
                relative_path = os.path.join("blobs", "sha256", file_name)
                layer_files.add(
                    os.path.normpath(relative_path)
                )  # Normalize path for consistency
    else:
        raise FileNotFoundError(
            f"The directory {sha256_dir} does not exist or is not a directory."
        )

    return layer_files


def extract_layers_and_files(client, image, tag, temp_dir):
    """
    Extract the layers and files from the Docker image.
    
    :param client: Docker client object.
    :param image: Name of the Docker image.
    :param tag: Tag of the Docker image.
    :param temp_dir: Directory to store the extracted layers and files.
    """    
    log.info(
        f"[INFO] Saving docker image {image}:{tag} to {os.path.join(temp_dir, 'image.tar')}"
    )
    pull_image(client, image, tag)

    image_obj = client.images.get(f"{image}:{tag}")
    with open(os.path.join(temp_dir, "image.tar"), "wb") as tar_file:
        for chunk in image_obj.save(named=True):
            tar_file.write(chunk)

    log.info(f"[INFO] Extracting contents from {os.path.join(temp_dir, 'image.tar')}")
    with tarfile.open(os.path.join(temp_dir, "image.tar"), "r") as tar:
        tar.extractall(os.path.join(temp_dir, "layers"))


def save_differences(old_files, new_files, output_file):
    """
    Save the differences between old and new files to a JSON file.

    :param old_files: Set of file paths from the old version.
    :param new_files: Set of file paths from the new version.
    :param output_file: Path to the JSON file where differences will be saved.
    """
    diff = {
        "added": list(new_files - old_files),
        "removed": list(old_files - new_files),
    }
    with open(output_file, "w") as f:
        json.dump(diff, f, indent=4)
    log.info(f"[SUCCESS] Differences saved to {output_file}")


def generate_diff(client, image, tag1, tag2):
    """
    Generate a diff tar file between two versions of a Docker image.
    
    :param client: Docker client object.
    :param image: Name of the Docker image.
    :param tag1: Old version tag.
    :param tag2: New version tag.    
    """    
    reg_name_removed_img = image.replace("/", "_").replace(
        "\\", "_"
    )  # Handle both separators
    log.info(f"[INFO] Processing image {image} with tags {tag1} and {tag2}")

    # Create temporary directories for extracting layers and storing differences
    temp_dir_r1 = tempfile.mkdtemp()
    temp_dir_r2 = tempfile.mkdtemp()
    temp_dir_diff = tempfile.mkdtemp()

    os.makedirs(os.path.join(temp_dir_diff, "blobs", "sha256"), exist_ok=True)

    try:
        # Extract layers and files for the old and new versions
        log.info(f"[INFO] Extracting layers for {image}:{tag1}")
        extract_layers_and_files(client, image, tag1, temp_dir_r1)

        log.info(f"[INFO] Extracting layers for {image}:{tag2}")
        extract_layers_and_files(client, image, tag2, temp_dir_r2)
        
        # Read the SHA256 layer files from the blobs/sha256 directories
        old_version_layers = read_from_blobs(os.path.join(temp_dir_r1, "layers"))
        new_version_layers = read_from_blobs(os.path.join(temp_dir_r2, "layers"))

        log.info(f"[INFO] Comparing layers between {image}:{tag1} and {image}:{tag2}")

        for curr_layer in new_version_layers:
            if curr_layer not in old_version_layers:
                log.info(
                    f"[INFO] Layer {curr_layer} is new or changed in {image}:{tag2}"
                )
                src = os.path.join(temp_dir_r2, "layers", curr_layer)
                dst = os.path.join(temp_dir_diff, "blobs", "sha256")
                shutil.copy(src, dst)

        # Copy manifest.json and repositories to the diff directory
        shutil.copy(os.path.join(temp_dir_r2, "layers", "manifest.json"), temp_dir_diff)
        shutil.copy(os.path.join(temp_dir_r2, "layers", "repositories"), temp_dir_diff)

        # Save differences between the old and new versions
        diff_file = os.path.join(temp_dir_diff, f"diff_{tag2}.json")
        save_differences(old_version_layers, new_version_layers, diff_file)

        diff_tar = os.path.join(
            DIFF_OUTPUT_DIR, f"{reg_name_removed_img}_diff_{tag2}.tar"
        )

        log.info(f"[INFO] Creating diff tar file {diff_tar}")
        with tarfile.open(diff_tar, "w") as tar:
            tar.add(temp_dir_diff, arcname="")

        log.info(f"[SUCCESS] Diff tar created successfully: {diff_tar}")

    finally:
        # Clean up temporary directories
        shutil.rmtree(temp_dir_r1)
        shutil.rmtree(temp_dir_r2)
        shutil.rmtree(temp_dir_diff)


def process_image(client, image, tag1, tag2):
    """
    Process the existing diff tar file and update it based on the new release.
    
    :param client: Docker client object.
    :param image: Name of the Docker image.
    :param tag1: Old version tag.
    :param tag2: New version tag.    
    """
    
    
    updated_image = image.replace("/", "_").replace("\\", "_")
    diff_tar = os.path.join(DIFF_OUTPUT_DIR, f"{updated_image}_diff_{tag2}.tar")

    if not os.path.exists(diff_tar):
        log.error(f"[ERROR] Diff tar file {diff_tar} does not exist")
        return

    log.info(f"[INFO] Processing image {image} with tags {tag1} and {tag2}")

    temp_dir_r1 = tempfile.mkdtemp()
    temp_dir_diff = tempfile.mkdtemp()
    temp_dir_updated_diff = tempfile.mkdtemp()

    os.makedirs(os.path.join(temp_dir_updated_diff, "blobs", "sha256"), exist_ok=True)

    try:
        log.info(
            f"[INFO] Saving docker image {image}:{tag1} to {os.path.join(temp_dir_r1, 'image_r1.tar')}"
        )
        pull_image(client, image, tag1)
        image_obj = client.images.get(f"{image}:{tag1}")
        with open(os.path.join(temp_dir_r1, "image_r1.tar"), "wb") as tar_file:
            for chunk in image_obj.save():
                tar_file.write(chunk)

        log.info(
            f"[INFO] Extracting contents from {os.path.join(temp_dir_r1, 'image_r1.tar')}"
        )
        with tarfile.open(os.path.join(temp_dir_r1, "image_r1.tar"), "r") as tar:
            tar.extractall(temp_dir_r1)

        os.remove(os.path.join(temp_dir_r1, "image_r1.tar"))

        log.info(f"[INFO] Extracting diff tar {diff_tar} to {temp_dir_diff}")
        with tarfile.open(diff_tar, "r") as tar:
            tar.extractall(temp_dir_diff)

        # read difference json file
        diff_json = os.path.join(temp_dir_diff, f"diff_{tag2}.json")
        if not os.path.exists(diff_json):
            log.error(f"[ERROR] {diff_json} not found in {diff_tar}")
            return

        # read layers from json file
        with open(diff_json) as f:
            layers_add = json.load(f)["added"]
        with open(diff_json) as f:
            layers_rm = json.load(f)["removed"]

        # manifest_json = os.path.join(temp_dir_diff, "manifest.json")
        # if not os.path.exists(manifest_json):
        #     log.error(f"[ERROR] {manifest_json} not found in {diff_tar}")
        #     return

        # with open(manifest_json) as f:
        #     layers = json.load(f)[0]["Layers"]

        # there is a problem with seperator in the layers it uses '/'
        # but it wont work on windows machine so handle it here
        # replace '/' with os.sep for every layers
        # layers = [layer.replace("/", os.sep) for layer in layers]

        log.info(f"[INFO] Preparing updated diff")
        os.makedirs(temp_dir_updated_diff, exist_ok=True)

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
            dest_dir = os.path.join(temp_dir_updated_diff, dir_name)
            if os.path.exists(src_dir):
                shutil.copy(src_dir, dest_dir)

        log.info(f"[INFO] Different layers were copied to the updated diff tar")
        
        # Copy remaining manifest and repository files to diff tar
        remaining_files = [
            os.path.join(temp_dir_r1, file_name)
            for file_name in os.listdir(temp_dir_r1)
        ]

        for dir_name in remaining_files:
            if os.path.isdir(dir_name):
                continue
            src_dir = os.path.join(temp_dir_diff, dir_name)
            dest_dir = os.path.join(
                temp_dir_updated_diff, os.path.relpath(dir_name, temp_dir_r1)
            )
            if os.path.exists(dest_dir):
                continue
            if os.path.exists(src_dir):
                shutil.copy(src_dir, dest_dir)

        shutil.copy(os.path.join(temp_dir_diff, "manifest.json"), temp_dir_updated_diff)

        old_version_layers = read_layers_from_manifest(
            os.path.join(temp_dir_r1, "manifest.json")
        )

        log.info(f"[INFO] Checking for missing layers...")

        # Check for layers that are missing in the diff tar
        for layer in layers_add:
            if layer not in extracted_dirs:
                log.info(
                    f"[INFO] Layer {layer} is missing in the diff tar. Copying from old version"
                )
                if layer in old_version_layers:
                    src = os.path.join(temp_dir_r1, layer)
                    dst = os.path.join(temp_dir_updated_diff, layer)
                    shutil.copy(src, dst)
                else:
                    log.error(
                        f"[ERROR] Layer {layer} is missing in the old version. Cannot proceed."
                    )
                    return
        
        log.info(f'[INFO] Checking for removed layers...')         
        
            
        updated_diff_tar = os.path.join(
            NEW_RELEASES_DIR, f"{updated_image}_diff_{tag2}.tar"
        )
        log.info(f"[SUCCESS] Creating updated diff tar file {updated_diff_tar}")
        with tarfile.open(updated_diff_tar, "w") as tar:
            tar.add(temp_dir_updated_diff, arcname="")

        log.info(f"[SUCCESS] Image loaded into the target environment repository")

    finally:
        shutil.rmtree(temp_dir_r1)
        shutil.rmtree(temp_dir_diff)
        shutil.rmtree(temp_dir_updated_diff)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        usage()

    option = sys.argv[1]
    image_list_json = sys.argv[2]

    if not os.path.isfile(image_list_json):
        log.error(f"[ERROR] File {image_list_json} does not exist")
        sys.exit(1)

    output_dir = DIFF_OUTPUT_DIR
    new_releases_dir = NEW_RELEASES_DIR

    if option == "--target":
        os.makedirs(new_releases_dir, exist_ok=True)

    if not os.path.isdir(output_dir):
        if option == "--dev":
            os.makedirs(output_dir, exist_ok=True)
        else:
            log.error(f"[ERROR] Output directory {output_dir} does not exist")
            sys.exit(1)

    client = docker.from_env()

    with open(image_list_json) as f:
        image_list = json.load(f)

    for entry in image_list:
        image = entry.get("Image")
        tag1 = entry.get("Old Version")
        tag2 = entry.get("New Version")

        if not image or not tag1 or not tag2:
            log.error(f"[ERROR] Missing image or tags in entry: {entry}")
            continue

        log.info(f"Image to be processed: {image}")
        log.info(f"from {tag1}")
        log.info(f"to {tag2}")

        if option == "--dev":
            generate_diff(client, image, tag1, tag2)
        elif option == "--target":
            process_image(client, image, tag1, tag2)
        elif option == "--help":
            usage()
        else:
            usage()
