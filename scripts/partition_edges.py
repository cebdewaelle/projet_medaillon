import os
import pandas as pd
import numpy as np
from math import ceil
import shutil


nom_fichier_nodes_parquet = "data/bronze/nodes.parquet"
nom_fichier_edges_parquet = "data/bronze/edges.parquet"

SILVER_PATH = "data/silver"
NOMBRE_SHARDS = 8


def partitionner_edges(
    df: pd.DataFrame, nombre_lignes_shards: int
) -> list[pd.DataFrame]:
    """
    Partitionne un DataFrame en nombre_lignes_shards.
    """
    partitions = [
        df.iloc[i : i + nombre_lignes_shards]
        for i in range(0, len(df), nombre_lignes_shards)
    ]
    return partitions


def ecrire_partitions(partitions: list[pd.DataFrame]):
    """
    Écrit chaque shard dans data/silver/shard={i}/edges.parquet
    """
    shard_n = 0
    for partition in partitions:
        repertoire_shard = f"{SILVER_PATH}/shard={shard_n}"
        os.mkdir(repertoire_shard)
        fichier_shard = f"{repertoire_shard}/edges.parquet"

        partition.to_parquet(fichier_shard, index=False)
        shard_n += 1
    print("Les fichiers shards ont été créés")


def copier_parquet_nodes():
    """
    Copie le fichier bronze/nodes.parquet dans silver/nodes.parquet
    """
    fichier_destination = f"{SILVER_PATH}/{nom_fichier_nodes_parquet.split('/')[-1]}"

    # Copier le fichier
    shutil.copy(nom_fichier_nodes_parquet, fichier_destination)


if __name__ == "__main__":
    df = pd.read_parquet(nom_fichier_edges_parquet)
    rows, cols = df.shape
    print(rows)

    # division du nombre de lignes du Dataframe en NOMBRE_SHARDS
    nb_lignes_shards = ceil(rows / NOMBRE_SHARDS)
    print(nb_lignes_shards)

    partitions = partitionner_edges(df, nb_lignes_shards)
    ecrire_partitions(partitions)

    copier_parquet_nodes()
