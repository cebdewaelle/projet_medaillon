import pandas as pd
import os
import time
from pathlib import Path


nom_fichier_nodes_parquet = "data/silver/nodes.parquet"
nom_fichier_edges_parquet = "edges.parquet"
nom_repertoires_edges_parquet = "data/silver/shard="
nom_repertoires_edges_csv = "data/gold/shard="
nom_fichier_nodes_csv = "data/gold/nodes.csv"
nom_fichier_edges_csv = "edges.csv"

NODES_COL_NAMES = {"id": "id:ID", "label": ":LABEL"}
EDGES_COL_NAMES = {"src": ":START_ID", "dst": ":END_ID", "rel": ":TYPE"}

NOMBRE_SHARDS = 8


def parquet_to_csv(parquet_file, csv_file, col_names):
    """
    Convertit un fichier parquet en un fichier CSV
    """
    parquet_path = Path(parquet_file)
    if not parquet_path.exists():
        print(f"Fichier inexistant : {parquet_file}")
        return

    df = pd.read_parquet(parquet_file)
    df.rename(columns=col_names, inplace=True)
    df.to_csv(csv_file, mode="w", header=True, index=False)

    print(f"Fichier parquet {parquet_file} a été convertis en csv")


if __name__ == "__main__":
    # Création de nodes.csv à partir de data/silver/nodes.parquet
    parquet_to_csv(nom_fichier_nodes_parquet, nom_fichier_nodes_csv, NODES_COL_NAMES)

    for i in range(NOMBRE_SHARDS):
        fichier_parquet = (
            f"{nom_repertoires_edges_parquet}{i}/{nom_fichier_edges_parquet}"
        )

        Path(f"{nom_repertoires_edges_csv}{i}").mkdir(parents=True, exist_ok=True)
        fichier_csv = f"{nom_repertoires_edges_csv}{i}/{nom_fichier_edges_csv}"

        parquet_to_csv(fichier_parquet, fichier_csv, EDGES_COL_NAMES)
