import json
import os
import shutil
import tarfile
import docker
import tempfile
import logging
import sys

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
log = logging.getLogger()

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

def pull_image(client, image, tag):
    try:
        client.images.get(f"{image}:{tag}")
    except docker.errors.ImageNotFound:
        log.info(f"[INFO] Docker image {image}:{tag} not found locally. Pulling from registry...")
        client.images.pull(image, tag)

def read_layers_from_manifest(manifest_json_path):
    """
    Read the layers from the manifest JSON file.

    :param manifest_json_path: Path to the manifest.json file.
    :return: List of layer directories.
    """
    with open(manifest_json_path, 'r') as f:
        manifest = json.load(f)
    
    # Extract layer directories from manifest JSON
    layer_dirs = set()
    for item in manifest:
        layers = item.get('Layers', [])
        for layer in layers:
            layer_dirs.add(layer)
    
    return layer_dirs

def extract_layers_and_files(client, image, tag, temp_dir):
    log.info(f"[INFO] Saving docker image {image}:{tag} to {temp_dir}/image.tar")
    pull_image(client, image, tag)
    
    image_obj = client.images.get(f"{image}:{tag}")
    with open(f"{temp_dir}/image.tar", 'wb') as tar_file:
        for chunk in image_obj.save():
            tar_file.write(chunk)
    
    log.info(f"[INFO] Extracting contents from {temp_dir}/image.tar")
    with tarfile.open(f"{temp_dir}/image.tar", 'r') as tar:
        tar.extractall(f"{temp_dir}/layers")

def generate_diff(client, image, tag1, tag2):
    reg_name_removed_img = image.replace("/", "_")
    log.info(f"[INFO] Processing image {image} with tags {tag1} and {tag2}")

    temp_dir_r1 = tempfile.mkdtemp()
    temp_dir_r2 = tempfile.mkdtemp()
    temp_dir_diff = tempfile.mkdtemp()

    os.makedirs(f"{temp_dir_diff}/blobs/sha256", exist_ok=True)
    
    try:
        log.info(f"[INFO] Extracting layers for {image}:{tag1}")
        extract_layers_and_files(client, image, tag1, temp_dir_r1)

        log.info(f"[INFO] Extracting layers for {image}:{tag2}")
        extract_layers_and_files(client, image, tag2, temp_dir_r2)


        # Extract layer directories from manifest JSON
        old_version_layers = read_layers_from_manifest(os.path.join(temp_dir_r1, "layers/manifest.json"))
        new_version_layers = read_layers_from_manifest(os.path.join(temp_dir_r2, "layers/manifest.json"))
                
        log.info(f"[INFO] Comparing layers between {image}:{tag1} and {image}:{tag2}")

        for curr_layer in new_version_layers:
            if curr_layer not in old_version_layers:
                log.info(f"[INFO] Layer {curr_layer} is new or changed in {image}:{tag2}")
                src = f"{temp_dir_r2}/layers/{curr_layer}"
                dst = f"{temp_dir_diff}/blobs/sha256"
                shutil.copy(src, dst)

        # Copy manifest.json to the diff directory
        shutil.copy(os.path.join(temp_dir_r2, "layers/manifest.json"), temp_dir_diff)
        
        shutil.copy(os.path.join(temp_dir_r2, "layers/repositories"), temp_dir_diff)
        
        diff_tar = f"output-diff-images/{reg_name_removed_img}_diff_{tag2}.tar"
        log.info(f"[INFO] Creating diff tar file {diff_tar}")
        with tarfile.open(diff_tar, "w") as tar:
            tar.add(temp_dir_diff, arcname=".")

        log.info(f"[SUCCESS] Diff tar created successfully: {diff_tar}")

    finally:
        shutil.rmtree(temp_dir_r1)
        shutil.rmtree(temp_dir_r2)
        shutil.rmtree(temp_dir_diff)

def process_image(client, image, tag1, tag2):
    sanitized_image = image.replace("/", "_")
    diff_tar = f"output-diff-images/{sanitized_image}_diff_{tag2}.tar"

    if not os.path.exists(diff_tar):
        log.error(f"[ERROR] Diff tar file {diff_tar} does not exist")
        return

    log.info(f"[INFO] Processing image {image} with tags {tag1} and {tag2}")

    temp_dir_r1 = tempfile.mkdtemp()
    temp_dir_diff = tempfile.mkdtemp()
    temp_dir_updated_diff = tempfile.mkdtemp()
    
    os.makedirs(f"{temp_dir_updated_diff}/blobs/sha256", exist_ok=True)

    try:
        log.info(f"[INFO] Saving docker image {image}:{tag1} to {temp_dir_r1}/image_r1.tar")
        pull_image(client, image, tag1)
        image_obj = client.images.get(f"{image}:{tag1}")
        with open(f"{temp_dir_r1}/image_r1.tar", 'wb') as tar_file:
            for chunk in image_obj.save():
                tar_file.write(chunk)

        log.info(f"[INFO] Extracting contents from {temp_dir_r1}/image_r1.tar")
        with tarfile.open(f"{temp_dir_r1}/image_r1.tar", 'r') as tar:
            tar.extractall(temp_dir_r1)

        log.info(f"[INFO] Extracting diff tar {diff_tar} to {temp_dir_diff}")
        with tarfile.open(diff_tar, 'r') as tar:
            tar.extractall(temp_dir_diff)

        manifest_json = os.path.join(temp_dir_diff, "manifest.json")
        if not os.path.exists(manifest_json):
            log.error(f"[ERROR] {manifest_json} not found in {diff_tar}")
            return

        # layers is the list of sha256 hashes of the layers in the diff tar
        with open(manifest_json) as f:
            layers = json.load(f)[0]['Layers']

   
        
        # r1_dirs = os.listdir(temp_dir_r1)

        log.info(f"[INFO] Preparing updated diff")
        os.makedirs(temp_dir_updated_diff, exist_ok=True)
        
        extracted_dirs = sorted([os.path.join('blobs/sha256', file_name) for file_name in os.listdir(f'{temp_dir_diff}/blobs/sha256')])      
        
        for dir_name in extracted_dirs:
            os.makedirs(os.path.join(temp_dir_updated_diff, 'blobs/sha256'), exist_ok=True)
            src_dir = os.path.join(temp_dir_diff, dir_name)
            dest_dir = os.path.join(temp_dir_updated_diff, dir_name)
            
            if os.path.exists(src_dir):
                shutil.copy(src_dir, dest_dir)  
        
        # #append remaining files in the temp_dir_diff directory
        remaining_files = ([os.path.join(temp_dir_r1, file_name) for file_name in os.listdir(f'{temp_dir_r1}')]) 

        for dir_name in remaining_files:
            if os.path.isdir(dir_name):
                continue
            
            src_dir = os.path.join(temp_dir_diff, dir_name)
            dest_dir = os.path.join(temp_dir_updated_diff, dir_name.removeprefix(temp_dir_r1)[1:])
            
            if os.path.exists(dest_dir):
                continue
            
            if os.path.exists(src_dir):
                shutil.copy(src_dir, dest_dir)
        
                
        # Copy manifest file of the new image to the updated diff directory
        shutil.copy(os.path.join(temp_dir_diff, "manifest.json"), temp_dir_updated_diff)
                
        old_version_layers = read_layers_from_manifest(os.path.join(temp_dir_r1, "manifest.json"))
        
        log.info(f"[INFO] Checking for missing layers...")
        
        # iterate through the layers in the new version's manifest file       
        for layer in layers: # updated layers
            # check if the layer in the difference
            if layer not in extracted_dirs:
                log.info(f"[INFO] Layer {layer} is missing in the diff tar. Copying from old version")
                
                # check if the old version has the layer
                if layer in old_version_layers:
                    src = os.path.join(temp_dir_r1, layer)
                    dst = os.path.join(temp_dir_updated_diff, layer)
                    shutil.copy(src, dst)
                else:
                    log.error(f"[ERROR] Layer {layer} is missing in the old version. Cannot proceed.")
                    return
            
            

        updated_diff_tar = f"new-releases/{sanitized_image}_diff_{tag2}.tar"
        log.info(f"[SUCCESS] Creating updated diff tar file {updated_diff_tar}")
        with tarfile.open(updated_diff_tar, "w") as tar:
            tar.add(temp_dir_updated_diff, arcname=".")

        # log.info(f"[INFO] Loading new release to the docker repository")
        # client.images.load(open(updated_diff_tar, 'rb').read())

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

    output_dir = "output-diff-images"
    new_releases_dir = "new-releases"

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
