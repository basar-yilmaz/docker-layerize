import json
import os
import shutil
import subprocess
import sys
import tempfile

LOGFILE = "script.log"

def log(message):
    with open(LOGFILE, "a") as log_file:
        print(message)
        log_file.write(message + "\n")

def usage():
    print("""
Usage: script.py [--dev | --target] <image-list.json>

Options:
  --dev     Generate diff tar files between old and new image releases.
  --target  Process existing diff tar files and update them based on the new release. "output-diff-images" will not change.
            Generated new releases images.tar can be viewed under "new-releases" directory.
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
  script.py --dev json-reg-images.json
  script.py --target image-list.json
""")
    sys.exit(1)

def pull_image(image, tag):
    if subprocess.run(["docker", "image", "inspect", f"{image}:{tag}"], stdout=subprocess.PIPE, stderr=subprocess.PIPE).returncode != 0:
        log(f"[INFO] Docker image {image}:{tag} could not be found in the repository. Pulling from registry...")
        subprocess.run(["docker", "pull", f"{image}:{tag}"])

def extract_layers_and_files(image, tag, temp_dir):
    log(f"[INFO] Saving docker image {image}:{tag} to {temp_dir}/image.tar")
    pull_image(image, tag)
    if subprocess.run(["docker", "save", "-o", f"{temp_dir}/image.tar", f"{image}:{tag}"]).returncode != 0:
        log(f"[ERROR] Failed to save Docker image {image}:{tag}")
        sys.exit(1)

    log(f"[INFO] Extracting contents from {temp_dir}/image.tar")
    os.makedirs(f"{temp_dir}/layers", exist_ok=True)
    if subprocess.run(["tar", "-xf", f"{temp_dir}/image.tar", "-C", f"{temp_dir}/layers"]).returncode != 0:
        log(f"[ERROR] Failed to extract Docker image {image}:{tag}")
        sys.exit(1)

def generate_diff(image, tag1, tag2):
    reg_name_removed_img = image.replace("/", "_")
    log(f"[INFO] Processing image {image} with tags {tag1} and {tag2}")

    temp_dir_r1 = tempfile.mkdtemp()
    temp_dir_r2 = tempfile.mkdtemp()
    temp_dir_diff = tempfile.mkdtemp()

    try:
        log(f"[INFO] Extracting layers for {image}:{tag1}")
        extract_layers_and_files(image, tag1, temp_dir_r1)

        log(f"[INFO] Extracting layers for {image}:{tag2}")
        extract_layers_and_files(image, tag2, temp_dir_r2)

        layer_dirs_r1 = os.listdir(f"{temp_dir_r1}/layers")
        layer_dirs_r2 = os.listdir(f"{temp_dir_r2}/layers")

        for layer_dir in layer_dirs_r2:
            if layer_dir not in layer_dirs_r1:
                log(f"[INFO] Layer {layer_dir} is new or changed in {image}:{tag2}")
                shutil.copytree(f"{temp_dir_r2}/layers/{layer_dir}", f"{temp_dir_diff}/{layer_dir}")

                for json_file in os.listdir(f"{temp_dir_r2}/layers/{layer_dir}"):
                    if json_file.endswith(".json"):
                        shutil.copy(f"{temp_dir_r2}/layers/{layer_dir}/{json_file}", temp_dir_diff)

        shutil.copy(f"{temp_dir_r2}/layers/manifest.json", temp_dir_diff)
        shutil.copy(f"{temp_dir_r2}/layers/repositories", temp_dir_diff)

        diff_tar = f"output-diff-images/{reg_name_removed_img}_diff_{tag2}.tar"
        log(f"[INFO] Creating diff tar file {diff_tar}")
        if subprocess.run(["tar", "-cf", diff_tar, "-C", temp_dir_diff, "."]).returncode != 0:
            log(f"[ERROR] Failed to create diff tar file {diff_tar}")
            sys.exit(1)

        log(f"[SUCCESS] Diff tar created successfully: {diff_tar}")

    finally:
        shutil.rmtree(temp_dir_r1)
        shutil.rmtree(temp_dir_r2)
        shutil.rmtree(temp_dir_diff)

def process_image(image, tag1, tag2):
    sanitized_image = image.replace("/", "_")
    diff_tar = f"output-diff-images/{sanitized_image}_diff_{tag2}.tar"

    if not os.path.exists(diff_tar):
        log(f"[ERROR] Diff tar file {diff_tar} does not exist")
        return

    log(f"[INFO] Processing image {image} with tags {tag1} and {tag2}")

    temp_dir_r1 = tempfile.mkdtemp()
    temp_dir_diff = tempfile.mkdtemp()
    temp_dir_updated_diff = tempfile.mkdtemp()

    try:
        log(f"[INFO] Saving docker image {image}:{tag1} to {temp_dir_r1}/image_r1.tar")
        subprocess.run(["docker", "save", "-o", f"{temp_dir_r1}/image_r1.tar", f"{image}:{tag1}"])

        log(f"[INFO] Extracting contents from {temp_dir_r1}/image_r1.tar")
        subprocess.run(["tar", "-xf", f"{temp_dir_r1}/image_r1.tar", "-C", temp_dir_r1])

        log(f"[INFO] Extracting diff tar {diff_tar} to {temp_dir_diff}")
        subprocess.run(["tar", "-xf", diff_tar, "-C", temp_dir_diff])

        manifest_json = os.path.join(temp_dir_diff, "manifest.json")
        if not os.path.exists(manifest_json):
            log(f"[ERROR] {manifest_json} not found in {diff_tar}")
            sys.exit(1)

        layer_dirs = subprocess.run(["jq", "-r", ".[] | .Layers[] | rtrimstr(\"/layer.tar\")", manifest_json], stdout=subprocess.PIPE).stdout.decode().splitlines()

        extracted_dirs = sorted(os.listdir(temp_dir_diff))
        r1_dirs = os.listdir(temp_dir_r1)

        log(f"[INFO] Preparing updated diff")
        os.makedirs(temp_dir_updated_diff, exist_ok=True)

        for dir in extracted_dirs:
            shutil.copytree(os.path.join(temp_dir_diff, dir), os.path.join(temp_dir_updated_diff, dir))

        log(f"[INFO] Checking for missing layers...")
        for layer in layer_dirs:
            if layer not in extracted_dirs:
                log(f"[INFO] Layer {layer} is listed in manifest.json but not found in {diff_tar}")
                if layer in r1_dirs:
                    log(f"[INFO] Copying missing layer {layer} from image:{tag1} to diff.tar")
                    shutil.copytree(os.path.join(temp_dir_r1, layer), os.path.join(temp_dir_updated_diff, layer))
                else:
                    log(f"[WARNING] Layer directory {layer} is not found in image:{tag1}")

        for json_file in os.listdir(temp_dir_diff):
            if json_file.endswith(".json"):
                shutil.copy(os.path.join(temp_dir_diff, json_file), temp_dir_updated_diff)

        shutil.copy(os.path.join(temp_dir_diff, "manifest.json"), temp_dir_updated_diff)
        shutil.copy(os.path.join(temp_dir_diff, "repositories"), temp_dir_updated_diff)

        updated_diff_tar = f"new-releases/{sanitized_image}_diff_{tag2}.tar"
        log(f"[SUCCESS] Creating updated diff tar file {updated_diff_tar}")
        if subprocess.run(["tar", "-cf", updated_diff_tar, "-C", temp_dir_updated_diff, "."]).returncode != 0:
            log(f"[ERROR] Failed to create updated diff tar file {updated_diff_tar}")
            sys.exit(1)

        log(f"[INFO] Loading new release to the docker repository")
        subprocess.run(["docker", "load", "--input", diff_tar])
        log(f"[SUCCESS] Image loaded into the target environment repository")

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
        log(f"[ERROR] File {image_list_json} does not exist")
        sys.exit(1)

    output_dir = "output-diff-images"
    new_releases_dir = "new-releases"

    if option == "--target":
        os.makedirs(new_releases_dir, exist_ok=True)

    if not os.path.isdir(output_dir):
        if option == "--dev":
            os.makedirs(output_dir, exist_ok=True)
        else:
            log(f"[ERROR] Output directory {output_dir} does not exist")
            sys.exit(1)

    with open(image_list_json) as f:
        image_list = json.load(f)

    for entry in image_list:
        image = entry.get("Image")
        tag1 = entry.get("Old Version")
        tag2 = entry.get("New Version")

        if not image or not tag1 or not tag2:
            log(f"[ERROR] Missing image or tags in entry: {entry}")
            continue

        log(f"Image to be processed: {image}")
        log(f"from {tag1}")
        log(f"to {tag2}")

        if option == "--dev":
            generate_diff(image, tag1, tag2)
        elif option == "--target":
            process_image(image, tag1, tag2)
        elif option == "--help":
            usage()
        else:
            usage()
