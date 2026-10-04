---
license: cc-by-sa-4.0
pipeline_tag: object-detection
datasets:
- PINGEcosystem/sss-crab-pot-detection-ds
language:
- en
metrics:
- accuracy
tags:
- side-scan-sonar
- sonar
- crab-pot
- object-detection
- yolo
- onnx
---

# 🦀 Ghost Pot YOLO12
**YOLO12 model for side-scan sonar ghost pot detection**

[![Model on Hugging Face](https://img.shields.io/badge/Hugging%20Face-Model-yellow)](https://huggingface.co/PINGEcosystem/gv-yolo12)
[![Dataset on Hugging Face](https://img.shields.io/badge/Hugging%20Face-Dataset-orange)](https://huggingface.co/datasets/PINGEcosystem/sss-crab-pot-detection-ds)

This repository contains a fine-tuned YOLO-based object-detection model for identifying derelict crab pots in side-scan sonar imagery. The model was trained on the [PINGEcosystem/sss-crab-pot-detection-ds](https://huggingface.co/datasets/PINGEcosystem/sss-crab-pot-detection-ds) dataset, which contains annotated sonar imagery collected in Delaware's Inland Bays and Delaware Bay.

The model is part of the [GhostVision](https://github.com/PINGEcosystem/GhostVision) effort to support scalable detection and mapping of derelict fishing gear from acoustic imagery.

## 📜 Publication
Bodine, C. S., Baxevani, K., Abbasi, N., Wierzbicki, J., Christoph, O., Hughes, C., Bagoren, O., Hines, O., Greco, J., & Trembanis, A. (2026). GhostVision: Democratizing Derelict Gear Detection Using Low-Cost Sonar and Artificial Intelligence. Journal of Marine Science and Engineering, 14(10), 951. https://doi.org/10.3390/jmse14100951

## 🧠 Model Overview
- Architecture: YOLO12 Small (`yolov12s`)
- Task: Object detection
- Input modality: Side-scan sonar imagery
- Primary target class: Crab-Pot
- Training dataset: [PINGEcosystem/sss-crab-pot-detection-ds](https://huggingface.co/datasets/PINGEcosystem/sss-crab-pot-detection-ds)

This model was trained from a dataset that originally contained both `Crab-Pot` and `Maybe-Crab-Pot` labels. During preprocessing, ambiguous `Maybe-Pot` examples were omitted so the exported detector predicts a single foreground class:

- `Crab-Pot`

## 📦 Files
- `model.safetensors` - serialized model weights
- `weights.onnx` - ONNX export for portable inference
- `class_names.txt` - class label mapping used for export
- `model_type.json` - model/task metadata
- `environment.json` - training/export environment metadata
- `README.md` - model card and usage guidance

## 🗂️ Training Data
This model was trained using the [PINGEcosystem/sss-crab-pot-detection-ds](https://huggingface.co/datasets/PINGEcosystem/sss-crab-pot-detection-ds) dataset.

Dataset highlights:
- 6,674 annotated sonar images
- Consumer-grade Humminbird side-scan sonar imagery
- Bounding-box annotations in JSONL format
- Data collected from northern Rehoboth Bay and Indian River Bay, Delaware

Export metadata highlights:
- Model variant: `yolov12s`
- Input resolution: `640`
- Exported label space: `Crab-Pot`
- Dataset endpoint version: `rx5YMJ3d3GZMFobFJf3Y/5`

If you are looking for the source annotations, data splits, and schema details, use the dataset card above.

## 🎯 Intended Use
This model is intended for:
- automated detection of derelict crab pots in side-scan sonar imagery
- research workflows for marine debris mapping
- benchmarking sonar object-detection pipelines
- downstream human-in-the-loop review and prioritization

This model is not intended to replace field validation or expert review in operational removal workflows.

## 🧪 Inference Notes
The repository includes native weights exported to `model.safetensors` and an ONNX export in `weights.onnx` to support portable inference workflows.

The exported label space is:

```text
Crab-Pot
```

Predictions should be interpreted in the context of sonar-specific variability such as substrate texture, tow geometry, acoustic shadowing, and target burial.

## 🚀 Usage

### 📡 Use with GhostVision
For end-to-end processing of side-scan sonar data, use this model through [GhostVision](https://github.com/PINGEcosystem/GhostVision), which handles sonar preprocessing, moving-window tiling, inference orchestration, optional tracking, and georeferencing.

Typical GhostVision workflow:
1. Install and launch GhostVision.
2. Open your sonar recording or batch folder.
3. Select the YOLO12 model in the model dropdown.
4. Run detection and review the exported detections, shapefiles, and waypoint products.

Within the current GhostVision codebase, this model is exposed through the known model alias `yolo12_v1` and resolved to the packaged model directory `ghostvision-models/5`.

### 💻 Use without GhostVision
If you only want to run the detector itself, use the exported `weights.onnx` file directly with ONNX Runtime.

```bash
pip install huggingface_hub onnxruntime pillow numpy
```

```python
from huggingface_hub import hf_hub_download
import numpy as np
from PIL import Image
import onnxruntime as ort

repo_id = "PINGEcosystem/gv-yolo12"
model_path = hf_hub_download(repo_id=repo_id, filename="weights.onnx")

session = ort.InferenceSession(model_path, providers=["CPUExecutionProvider"])
input_name = session.get_inputs()[0].name

image = Image.open("path/to/sonar-image.jpg").convert("RGB").resize((512, 512))
image_np = np.asarray(image, dtype=np.float32) / 255.0
image_np = np.transpose(image_np, (2, 0, 1))[None, ...]

raw_outputs = session.run(None, {input_name: image_np})
output_map = {
	output_meta.name: output_value
	for output_meta, output_value in zip(session.get_outputs(), raw_outputs)
}

for name, value in output_map.items():
	print(name, value.shape)
```

This direct ONNX path is useful when you want to embed the detector in another application or build your own post-processing pipeline. The exact output tensors depend on the export format, so inspect the returned names and shapes before writing box filtering and visualization code.

## ⚠️ Limitations
- Performance may degrade on sonar systems, substrates, or regions that differ from the Delaware training domain.
- Small, partially buried, or weak-return targets may be missed.
- Sonar artifacts and hard-bottom features may produce false positives.
- The model predicts only the retained foreground class used during export.
- Outputs should be reviewed by domain experts before management or removal decisions are made.

## 🔗 Related Resources
- Dataset: [PINGEcosystem/sss-crab-pot-detection-ds](https://huggingface.co/datasets/PINGEcosystem/sss-crab-pot-detection-ds)
- GhostVision project: [PINGEcosystem/GhostVision](https://github.com/PINGEcosystem/GhostVision)
- RF-DETR companion model: [PINGEcosystem/gv-rf-detr](https://huggingface.co/PINGEcosystem/gv-rf-detr)
- YOLO26 companion model: [PINGEcosystem/gv-yolo26](https://huggingface.co/PINGEcosystem/gv-yolo26)

## 📜 License
This model card and associated artifacts are released under the license specified in this repository metadata.

## 🙌 Acknowledgments
This work was developed with support from:
- University of Delaware -- Center for Coastal Sediments Hydrodynamics and Engineering Lab (CSHEL)
- Delaware Sea Grant
- 2024 Autonomous Systems Bootcamp
- NOAA's Project ABLE
- NOAA Marine Debris Program
- Delaware Department of Natural Resources and Environmental Control (DNREC)
- Community volunteers participating in ghost-gear surveys
