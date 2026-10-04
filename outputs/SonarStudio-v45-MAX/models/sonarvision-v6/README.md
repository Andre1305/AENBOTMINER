---
language:
- en
license: apache-2.0
library_name: onnx
pipeline_tag: object-detection
tags:
- sonar
- side-scan-sonar
- marine-debris
- underwater-detection
- yolo
- yolov8
- yolov8-esi
- edge-ai
- onnx
- computer-vision
- acoustic-sensing
- defense-tech
---

# 🌊 YOLOv8-ESI (v6): Multi-Class Side-Scan Sonar Object Detection

[![GitHub Repository](https://img.shields.io/badge/GitHub-Dinoman67%2Fsonarvision-blue?logo=github)](https://github.com/Dinoman67/sonarvision)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-yellow.svg)](https://www.apache.org/licenses/LICENSE-2.0)
[![Framework: ONNX](https://img.shields.io/badge/Framework-ONNX-orange.svg)](https://onnx.ai/)
[![Pipeline: Object Detection](https://img.shields.io/badge/Task-Object%20Detection-brightgreen.svg)](#)

> **Official Project Source Code & Documentation:**  
> 🔗 **[https://github.com/Dinoman67/sonarvision](https://github.com/Dinoman67/sonarvision)**  
> *(Refer to the main repository for real-time edge streaming pipelines, web dashboard, PDF forensic report generator, synthetic acoustic augmentations, and optional supporting features such as C2 (NMEA/KML) exports, XTF/slant-range helpers, and a simulated live-waterfall demo view)*

---

## 📌 Overview

**YOLOv8-ESI v6** is an edge-optimized, attention-enhanced deep learning model designed specifically for acoustic **Side-Scan Sonar (SSS)** imagery. Built for autonomous underwater vehicles (AUVs), uncrewed surface vessels (USVs), and edge compute modules (Raspberry Pi 3/4/5, Jetson Nano/Orin), it directly tackles acoustic speckle noise, gain attenuation, nadir dropout, and acoustic shadow geometry.

Unlike standard RGB detectors, YOLOv8-ESI embeds **Squeeze-and-Excitation (SE) channel attention** into its C2f feature aggregation layers, allowing the network to selectively amplify subtle acoustic highlights and suppress reverberant seafloor clutter.

### Key Highlights:
- **Parameter Count:** 3,030,108 parameters (~3.03M)
- **Computational Cost:** 8.1 GFLOPs at 256×256 input
- **Inference Latency:** ~2.4ms on GPU, <45ms on edge ARM CPUs
- **Precision:** FP16 (5.88 MB) & FP32 (11.67 MB)
- **4 Tactical Target Classes:**
  - `0: unknown_debris` — Submerged marine debris, industrial waste, scrap, and obstacles (NOAA Klein 5000 SSS)
  - `1: airplane` — Submerged aircraft wrecks and fuselages
  - `2: mine` — Naval bottom mines, tethered shapes, and NOMBOs (MILCO Klein 3500 MCM)
  - `3: wreck` — Sunken vessel hulls and structural shipwrecks

Developed for **Smart India Hackathon 2026 | SIH26057 (Ministry of Earth Sciences / NIOT)** — AI-Powered Automated Underwater Marine Debris and Anomaly Detection System using Side-Scan Sonar Imagery.

---

## 📊 Benchmark & Evaluation (v6 Test Set)

Evaluated on a strictly held-out test partition of **962 side-scan sonar images** (231 annotated object instances) across diverse seabed topographies:

### Per-Class Test Performance (962 Images)

| Class ID | Class Name | Test AP50 | Precision | Recall | Target Sensor / Source |
| :---: | :--- | :---: | :---: | :---: | :--- |
| `0` | **unknown_debris** | **0.8185** | **81.88%** | **78.69%** | NOAA Klein 5000 Multi-Pass Surveys |
| `1` | **airplane** | **0.4950** | 57.20% | 69.20% | Kaggle SSS Benchmark |
| `2` | **mine** | **0.3862** | **70.73%** | 29.59% | MILCO Klein 3500 Mine Countermeasures |
| `3` | **wreck** | **0.7173** | 80.03% | 60.35% | Kaggle SSS Benchmark |
| **ALL** | **Overall Model** | **0.6042** | **66.87%** | **62.37%** | **F1 Score = 0.6454 (mAP50-95 = 0.3348)** |

### Scope Note

Built for multi-threat statements (debris, MCM, SAR): four classes map onto
shipwrecks, pipes, cylinders, and entangled nets plus geotagged reporting.
On ghost nets specifically — established science (GhostNetZero, Microsoft
Research + WWF 2025, 412 private expert segments) with no open benchmark
available: this model trains on real survey acoustics only (no rendered
objects) and serves net-like contacts through `unknown_debris`. Metrics
above are held-out test with per-class breakdown; see the GitHub repository
for the full frame-vs-pass generalization analysis.

---

## 📦 Model Artifacts in this Repository

| File | Type | Precision | Size | Description |
| :--- | :---: | :---: | :---: | :--- |
| [`yolo_esi_v6_fp16.onnx`](./yolo_esi_v6_fp16.onnx) | Model | Float16 | **5.88 MB** | **Production edge inference (Raspberry Pi 4/5, Jetson, Web)** |
| [`yolo_esi_v6_fp32.onnx`](./yolo_esi_v6_fp32.onnx) | Model | Float32 | **11.67 MB** | Desktop / Server CPU inference, testing, and validation |
| [`yolo_esi_core_debris_fp16.onnx`](./yolo_esi_core_debris_fp16.onnx) | Model | Float16 | **6.16 MB** | Single-class marine debris specialist (0.8837 mAP50 — **leaked reference: test passes seen in train; do not deploy, use v6 above**) |
| [`build_sss_models.py`](./build_sss_models.py) | Code | Python | 18 KB | PyTorch model builder implementing the SE-enhanced C2f backbone |
| [`sss_custom_modules.py`](./sss_custom_modules.py) | Code | Python | 11 KB | Custom PyTorch layers (`SEBlock`, `GhostConv`, `FastC2f`) |

---

## 💻 Quick Start & Inference

You can run inference using standard `onnxruntime` and `opencv-python`.

### 1. Installation
```bash
pip install onnxruntime opencv-python numpy huggingface_hub
```

### 2. Download from Hugging Face

```python
import cv2
import numpy as np
import onnxruntime as ort
from huggingface_hub import hf_hub_download

REPO_ID = "Dinoman1221/sonarvision-yolov8-esi-v6"

model_path = hf_hub_download(
    repo_id=REPO_ID,
    filename="yolo_esi_v6_fp16.onnx",
)

print(f"Model downloaded to: {model_path}")
```

### 3. Run Inference on a Sonar Image
```python
CLASSES = ["unknown_debris", "airplane", "mine", "wreck"]
CONF_THRESHOLD = 0.25
IOU_THRESHOLD = 0.45
IMG_SIZE = 256

# Initialize ONNX Session
session = ort.InferenceSession(model_path, providers=['CPUExecutionProvider'])
input_name = session.get_inputs()[0].name

def letterbox(img, target_size=256):
    h, w = img.shape[:2]
    scale = min(target_size / h, target_size / w)
    nh, nw = int(round(h * scale)), int(round(w * scale))
    resized = cv2.resize(img, (nw, nh), interpolation=cv2.INTER_LINEAR)
    canvas = np.full((target_size, target_size, 3), 114, dtype=np.uint8)
    dx, dy = (target_size - nw) // 2, (target_size - nh) // 2
    canvas[dy:dy+nh, dx:dx+nw] = resized
    return canvas, scale, dx, dy

# Load image
raw_img = cv2.imread("sonar_sample.png")
input_tensor, scale, dx, dy = letterbox(raw_img, IMG_SIZE)
input_tensor = input_tensor.astype(np.float32) / 255.0
input_tensor = np.transpose(input_tensor, (2, 0, 1))[np.newaxis, ...]

# Predict
outputs = session.run(None, {input_name: input_tensor})[0]
predictions = np.squeeze(outputs).T # shape: [num_boxes, 8] -> (x, y, w, h, cls0..cls3)

boxes, scores, class_ids = [], [], []
for row in predictions:
    cls_scores = row[4:]
    cls_id = int(np.argmax(cls_scores))
    conf = float(cls_scores[cls_id])
    if conf > CONF_THRESHOLD:
        cx, cy, bw, bh = row[:4]
        # Rescale back to original image
        x1 = max(0, int(((cx - bw / 2) - dx) / scale))
        y1 = max(0, int(((cy - bh / 2) - dy) / scale))
        x2 = min(raw_img.shape[1], int(((cx + bw / 2) - dx) / scale))
        y2 = min(raw_img.shape[0], int(((cy + bh / 2) - dy) / scale))
        boxes.append([x1, y1, x2 - x1, y2 - y1])
        scores.append(conf)
        class_ids.append(cls_id)

indices = cv2.dnn.NMSBoxes(boxes, scores, CONF_THRESHOLD, IOU_THRESHOLD)
for idx in indices:
    i = idx if isinstance(idx, (int, np.integer)) else idx[0]
    bx, by, bw, bh = boxes[i]
    label = f"{CLASSES[class_ids[i]]} {scores[i]:.2f}"
    cv2.rectangle(raw_img, (bx, by), (bx + bw, by + bh), (0, 255, 0), 2)
    cv2.putText(raw_img, label, (bx, max(by - 5, 15)), cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0, 255, 0), 2)

cv2.imwrite("sonar_detection_result.png", raw_img)
print("Detection complete! Saved to sonar_detection_result.png")
```

---

## 🧩 Application Compatibility Note

This model card covers weights and benchmarks only (unchanged). The full KADAL application repository also includes optional supporting features alongside the core detector — acoustic context helpers, file-ingest utilities, NMEA/KML export helpers, operator review queue with CSV/JSON verdict stamping, single-ZIP evidence bundles, per-detection position uncertainty, and a simulated live-waterfall demo view — documented in the GitHub `README.md`.

---

## 📜 License & Citation

This project is distributed under the **Apache 2.0 License**.

```bibtex
@software{kadal2026,
  author = {Ashish S and Team KADAL},
  title = {KADAL: Autonomous Edge-AI Detection and Forensic Reporting for Side-Scan Sonar Imagery},
  year = {2026},
  publisher = {Hugging Face / GitHub},
  url = {https://github.com/Dinoman67/sonarvision}
}
```
