This project implements a question answering pipeline using the Stanford Question Answering Dataset (SQuAD). The goal is to explore dense retrieval and Retrieval-Augmented Generation (RAG) techniques, and to evaluate how different indexing and retrieval hyperparameters affect performance.

The workflow includes preprocessing the SQuAD dataset, indexing passages using ChromaDB, evaluating retrieval accuracy with the F1 score, and building a RAG-based QA system. The project also provides scripts to experiment with key hyperparameters such as chunk size, chunk overlap, embedding model, and the number of retrieved source chunks.

This repository is intended as a hands-on exercise to understand the practical components of modern QA systems, from data preprocessing to retrieval evaluation and answer generation.
