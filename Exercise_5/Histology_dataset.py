# Install required packages
!pip install torch transformers datasets Pillow umap-learn
!pip install s3fs tifffile imagecodecs zarr scikit-image
!pip install matplotlib seaborn

try:
    from umap.umap_ import UMAP
except ImportError:
    from umap import UMAP

from PIL import Image
import torch
from transformers import AutoImageProcessor, AutoModel
import matplotlib.pyplot as plt
from datasets import load_dataset
from tqdm import tqdm
import numpy as np
import io

# Set device
device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')

# Load dataset
ds_train = load_dataset("dakomura/tcga-ut", "internal", split="train", streaming=True)
ds_train = ds_train.filter(lambda sample: "Lung" in sample["json"]["label"])

ds_test = load_dataset("dakomura/tcga-ut", "internal", split="test", streaming=True)
ds_test = ds_test.filter(lambda sample: "Lung" in sample["json"]["label"])

# Load phikon-v2
processor = AutoImageProcessor.from_pretrained("owkin/phikon-v2", use_fast=True)
model = AutoModel.from_pretrained("owkin/phikon-v2")
model.eval()
model.to(device)

def load_and_process_image(sample):
    image_bytes = sample["jpg"]
    image = Image.open(io.BytesIO(image_bytes))
    return {
        "image": image,
        "label": sample["json"]["label"],
        "patient_id": sample["__key__"][:12]
    }

ds_train_processed = ds_train.map(load_and_process_image, remove_columns=["jpg", "json", "__key__", "__url__"])
ds_test_processed = ds_test.map(load_and_process_image, remove_columns=["jpg", "json", "__key__", "__url__"])

# Exercise 1: Visualize images
iter_ds = iter(ds_train_processed)
fig, axes = plt.subplots(2, 3, figsize=(12, 8))
for i in range(6):
    sample = next(iter_ds)
    axes[i//3, i%3].imshow(sample["image"])
    axes[i//3, i%3].set_title(f"{sample['label']}\n{sample['patient_id']}")
    axes[i//3, i%3].axis('off')
plt.tight_layout()
plt.show()

# Task 1.1: Linear Probing
ds_train_processed = ds_train_processed.with_format("torch")
ds_test_processed = ds_test_processed.with_format("torch")

BATCH_SIZE = 32
EMBEDDING_SIZE = 1024
NR_BATCHES = 100

dl_train = torch.utils.data.DataLoader(
    ds_train_processed,
    batch_size=BATCH_SIZE,
    shuffle=False,
    num_workers=8,
)

embeddings = torch.zeros((BATCH_SIZE * NR_BATCHES, EMBEDDING_SIZE))
labels = []
patient_ids = []

with torch.inference_mode():
    with torch.autocast(device.type, torch.bfloat16):
        for i, batch in enumerate(tqdm(dl_train, total=NR_BATCHES)):
            inputs = processor(batch["image"], return_tensors="pt").to(device)
            outputs = model(**inputs)
            batch_embeddings = outputs.last_hidden_state[:, 0, :]
            embeddings[i*BATCH_SIZE:(i+1)*BATCH_SIZE] = batch_embeddings.cpu()
            labels.extend(batch["label"])
            patient_ids.extend(batch["patient_id"])
            
            if i == (NR_BATCHES - 1):
                break

# Exercise 3: UMAP visualization
import seaborn as sns
labels = np.array(labels)
patient_ids = np.array(patient_ids)

# UMAP for all data
umap_all = UMAP(n_neighbors=30, min_dist=0.3, n_components=2, random_state=42)
embeddings_2d = umap_all.fit_transform(embeddings.numpy())

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

# Color by tumor type
unique_labels = np.unique(labels)
colors = plt.cm.Set3(np.linspace(0, 1, len(unique_labels)))
label_to_color = {label: colors[i] for i, label in enumerate(unique_labels)}

for label in unique_labels:
    mask = labels == label
    ax1.scatter(embeddings_2d[mask, 0], embeddings_2d[mask, 1], 
                color=label_to_color[label], label=label, alpha=0.6, s=20)
ax1.set_title("UMAP by Tumor Type")
ax1.legend()

# Color by patient
selected_patients = ['TCGA-33-4582', 'TCGA-55-8614', 'TCGA-77-8138', 'TCGA-71-8520']
mask = np.isin(patient_ids, selected_patients)
colors = plt.cm.tab10(np.linspace(0, 1, len(selected_patients)))
patient_to_color = {patient: colors[i] for i, patient in enumerate(selected_patients)}

for patient in selected_patients:
    patient_mask = (patient_ids == patient) & mask
    ax2.scatter(embeddings_2d[patient_mask, 0], embeddings_2d[patient_mask, 1],
                color=patient_to_color[patient], label=patient, alpha=0.6, s=20)
ax2.set_title("UMAP by Patient")
ax2.legend()

plt.tight_layout()
plt.show()

# Logistic Regression
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, f1_score, confusion_matrix

# Convert labels to binary
label_map = {"Lung adenocarcinoma": 0, "Lung squamous cell carcinoma": 1}
y_train = np.array([label_map[label] for label in labels])

# Train LR
lr_model = LogisticRegression(random_state=42, max_iter=1000)
lr_model.fit(embeddings.numpy(), y_train)

# Test embeddings
dl_test = torch.utils.data.DataLoader(
    ds_test_processed,
    batch_size=BATCH_SIZE,
    shuffle=False,
    num_workers=8,
)

test_embeddings = []
test_labels = []
test_patient_ids = []

with torch.inference_mode():
    with torch.autocast(device.type, torch.bfloat16):
        for i, batch in enumerate(tqdm(dl_test, total=NR_BATCHES)):
            inputs = processor(batch["image"], return_tensors="pt").to(device)
            outputs = model(**inputs)
            batch_embeddings = outputs.last_hidden_state[:, 0, :]
            test_embeddings.append(batch_embeddings.cpu())
            test_labels.extend(batch["label"])
            test_patient_ids.extend(batch["patient_id"])
            
            if i == (NR_BATCHES - 1):
                break

test_embeddings = torch.cat(test_embeddings, dim=0)
y_test = np.array([label_map[label] for label in test_labels])
patient_ids_test = np.array(test_patient_ids)

# Predict and evaluate
lr_predictions = lr_model.predict(test_embeddings.numpy())

accuracy = accuracy_score(y_test, lr_predictions)
f1 = f1_score(y_test, lr_predictions)
cm = confusion_matrix(y_test, lr_predictions)

print(f"Accuracy: {accuracy:.3f}")
print(f"F1-Score: {f1:.3f}")
print(f"Confusion Matrix:\n{cm}")

# Task 1.2: Tissue-level classification
import pandas as pd

df = pd.DataFrame({"pid": patient_ids_test, "prediction": lr_predictions, "label": y_test})
df_majority_voting = df.groupby("pid").agg({"prediction": pd.Series.mode, "label": "first"})

predictions_tissue = df_majority_voting["prediction"].values
labels_tissue = df_majority_voting["label"].values

predictions_tissue = [p[0] if isinstance(p, np.ndarray) else p for p in predictions_tissue]

tissue_accuracy = accuracy_score(labels_tissue, predictions_tissue)
tissue_f1 = f1_score(labels_tissue, predictions_tissue)

print(f"\nTissue-level Accuracy: {tissue_accuracy:.3f}")
print(f"Tissue-level F1-Score: {tissue_f1:.3f}")

# Task 1.3: Whole-slide image analysis
import s3fs
import os
from skimage.transform import resize

fs = s3fs.S3FileSystem(anon=True)
s3_path = 'lin-2023-orion-crc/data/CRC10/18459_LSP10452_US_SCAN_OR_001__091355-registered.ome.tif'
local_filename = 'crc10_he_wsi.ome.tif'

if not os.path.exists(local_filename):
    fs.get(s3_path, local_filename)

print(f'Downloaded {local_filename}')

import tifffile

region = tifffile.imread(local_filename)
region = region[15000:25080, 5000:15080]
plt.figure(figsize=(10, 10))
plt.imshow(region)
plt.axis('off')
plt.show()

# Slide window embedding
embeddings_cls = torch.zeros((45, 45, EMBEDDING_SIZE))
embeddings_ps = torch.zeros((630, 630, EMBEDDING_SIZE))

region_x, region_y = region.shape[:2]
patch_size = 224

with torch.inference_mode():
    with torch.autocast(device.type, torch.bfloat16):
        for x in range(0, region_x, patch_size):
            for y in range(0, region_y, patch_size):
                if x + patch_size <= region_x and y + patch_size <= region_y:
                    crop = region[x:x+patch_size, y:y+patch_size]
                    crop_pil = Image.fromarray(crop)
                    inputs = processor(crop_pil, return_tensors="pt").to(device)
                    outputs = model(**inputs)
                    
                    cls_idx_x = x // patch_size
                    cls_idx_y = y // patch_size
                    embeddings_cls[cls_idx_x, cls_idx_y] = outputs.last_hidden_state[:, 0, :].cpu()
                    
                    patch_tokens = outputs.last_hidden_state[:, 1:, :].cpu()
                    patch_tokens = patch_tokens.reshape((14, 14, EMBEDDING_SIZE))
                    start_x = cls_idx_x * 14
                    start_y = cls_idx_y * 14
                    embeddings_ps[start_x:start_x+14, start_y:start_y+14] = patch_tokens

# Cosine similarities
import torch.nn.functional as F

# Top left CLS
cls_00 = embeddings_cls[0, 0].unsqueeze(0)
vectors_all_cls = embeddings_cls.view(-1, EMBEDDING_SIZE)
cosine_sim_cls_00 = F.cosine_similarity(cls_00, vectors_all_cls, dim=1)
cosine_sim_cls_00 = cosine_sim_cls_00.reshape(embeddings_cls.shape[:2])

# Bottom right CLS
cls_nn = embeddings_cls[-1, -1].unsqueeze(0)
cosine_sim_cls_nn = F.cosine_similarity(cls_nn, vectors_all_cls, dim=1)
cosine_sim_cls_nn = cosine_sim_cls_nn.reshape(embeddings_cls.shape[:2])

# Patch similarities
ps_00 = torch.mean(embeddings_ps[:14, :14], dim=(0, 1)).unsqueeze(0)
vectors_all_ps = embeddings_ps.view(-1, EMBEDDING_SIZE)
cosine_sim_ps_00 = F.cosine_similarity(ps_00, vectors_all_ps, dim=1)
cosine_sim_ps_00 = cosine_sim_ps_00.reshape(embeddings_ps.shape[:2])

ps_nn = torch.mean(embeddings_ps[-14:, -14:], dim=(0, 1)).unsqueeze(0)
cosine_sim_ps_nn = F.cosine_similarity(ps_nn, vectors_all_ps, dim=1)
cosine_sim_ps_nn = cosine_sim_ps_nn.reshape(embeddings_ps.shape[:2])

# Visualization
cosine_sim_cls_00_resized = resize(cosine_sim_cls_00.numpy(), (region_x, region_y))
cosine_sim_cls_nn_resized = resize(cosine_sim_cls_nn.numpy(), (region_x, region_y))
cosine_sim_ps_00_resized = resize(cosine_sim_ps_00.numpy(), (region_x, region_y))
cosine_sim_ps_nn_resized = resize(cosine_sim_ps_nn.numpy(), (region_x, region_y))

plt.imshow(region[::10, ::10])
plt.axis('off')
plt.show()

fig, axs = plt.subplots(2, 3, figsize=(12, 5))
axs = axs.flatten()
axs[0].imshow(region[:224, :224])
axs[0].set_title("Top left crop (224x224)")
axs[1].imshow(region[::10, ::10])
axs[1].imshow(cosine_sim_cls_00_resized[::10, ::10], alpha=0.3, cmap="jet")
axs[2].imshow(region[::10, ::10])
axs[2].imshow(cosine_sim_ps_00_resized[::10, ::10], alpha=0.3, cmap="jet")
axs[3].imshow(region[-224:, -224:])
axs[3].set_title("Bottom right crop (224x224)")
axs[4].imshow(region[::10, ::10])
axs[4].imshow(cosine_sim_cls_nn_resized[::10, ::10], alpha=0.3, cmap="jet")
axs[5].imshow(region[::10, ::10])
axs[5].imshow(cosine_sim_ps_nn_resized[::10, ::10], alpha=0.3, cmap="jet")

axs[1].set_title("Cos. sim (CLS)")
axs[2].set_title("Cos. sim (Patch)")

for ax in axs:
    ax.axis('off')
plt.tight_layout()
plt.show()
