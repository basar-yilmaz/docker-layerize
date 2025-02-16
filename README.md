# docker-layers-update

## **1. image_tag_diff_tar.sh**

<p>
    Create a xxx.json file in the given format:
</p>

```json
[
  {
    "Image": "basaryilmaz/k8s-web-test",
    "Old Version": "1.9.9",
    "New Version": "2.0.0"
  }
]
```

#### Input Format

<p>
To run the script, use the following command:
</p>

```bash
./image_tag_diff_tar.sh xxx.json
```

<p>
This will create difference file in the /output-diff-images directory.
</p>

## **2. docker-image-patch.sh**

<p>
    Create a xxx.json file in the given format:
</p>

```json
[
  {
    "Image": "basaryilmaz/k8s-web-test",
    "Old Version": "1.9.9",
    "New Version": "2.0.0"
  }
]
```

#### Input Format for extracting differences

<p>
To run the <strong>difference</strong> script, use the following command:
</p>

```bash
./image_tag_diff_tar.sh --dev xxx.json
```

<p>
This will create difference file in the /output-diff-images directory.
</p>

#### Input Format for applying differences

<p>
To run the <strong>merge</strong> script, use the following command:
</p>

```bash
./image_tag_diff_tar.sh --target xxx.json
```
