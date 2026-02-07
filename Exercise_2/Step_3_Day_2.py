#Step_3_Day_2

# -*- coding: utf-8 -*-
"""
Task Sparse IR

Created on Tue November 11 10:42:53 2025

@author: agha
"""
import os
import json
import chromadb
import numpy as np
from tqdm import tqdm
from I_constants import *
from langchain_chroma import Chroma
from langchain_huggingface import HuggingFaceEmbeddings

test_data = json.load(open('squad_multiple_contexts.json', 'r')) 


def get_f1(true_doc: set, pred_doc: set) -> float:
    # We should have the same number of data, in the pred and in the true. 
    if not true_doc and not pred_doc:
        return 1.0  # Ambos vacíos → F1 perfecto
    if not true_doc or not pred_doc:
        return 0.0  # Uno vacío → F1=0
    
    # Intersección = documentos correctamente predichos
    tp = len(true_doc & pred_doc)
    
    # Precision: TP / pred_doc
    precision = tp / len(pred_doc)
    
    # Recall: TP / true_doc
    recall = tp / len(true_doc)
    
    if precision + recall == 0:
        return 0.0
    
    f1 = 2 * (precision * recall) / (precision + recall)
    return f1
if __name__ == "__main__":
    version = 1
    embeddings = HuggingFaceEmbeddings(model_name=embeddings_model_name, cache_folder=models_path)
    chroma_client = chromadb.PersistentClient(persist_path)
    db = Chroma(persist_directory=persist_path,
                embedding_function=embeddings,
                collection_name="test_collection",
                client=chroma_client
                )

    retriever = db.as_retriever(search_type="similarity",
                                search_kwargs={"k": K_source_chunks})

    f1s = []
    for entry in tqdm(test_data):
        query = entry['text']
        entry_prediction = [os.path.basename(doc.metadata["source"]) for doc in retriever.invoke(query)]
        entry_true = entry['sources']
        f1s.append(get_f1(set(entry_true), set(entry_prediction)))

    print("F1: %.2f"%(np.mean(f1s)))