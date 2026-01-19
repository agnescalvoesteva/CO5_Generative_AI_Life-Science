# Step_2
import os
import pickle
from tqdm import tqdm
from whoosh import index
from whoosh.fields import Schema, TEXT, ID
from step_1 import medline_folder
from whoosh.writing import AsyncWriter

# Schema
schema = Schema(
    pubmed_Id=ID(stored=True, unique=True),
    title=TEXT(stored=True),
    body=TEXT(stored=True),
    MesH=TEXT(stored=True)
)

index_dir = "pubmed_index"

def get_index():
    if not os.path.exists(index_dir): # make sure that the index_dir exist
        os.mkdir(index_dir)

    if not index.exists_in(index_dir):
        ix = index.create_in(index_dir, schema) # create the index
        print("Index created.") 
    else:
        ix = index.open_dir(index_dir) # open the index
        print("Index opened.")
    return ix

# add the documents that we extract in the step_1

def index_pubmed(ix):
    pkl_files = sorted([f for f in os.listdir(medline_folder) if f.endswith(".pkl")]) # we basically copy one of the function of the step_1

    for pkl_file in tqdm(pkl_files, desc="Indexing PubMed"): # we say for each file, put the infotmation inside the schema
        with open(os.path.join(medline_folder, pkl_file), "rb") as f:
            pmid2content = pickle.load(f)

        writer = AsyncWriter(ix)
        for pmid, (title, abstract, mesh_terms) in pmid2content.items(): # the information that we want to put inside the schema
            writer.update_document(
                pubmed_Id=str(pmid),
                title=str(title) if title else "",
                body=str(abstract) if abstract else "",
                MesH="; ".join(mesh_terms) if mesh_terms else ""
            )
        writer.commit()
    print("Indexing finished.")

if __name__ == "__main__":
    ix = get_index()
    index_pubmed(ix)

