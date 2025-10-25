import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


nom_fichier_nodes_raw = "data/raw/nodes.csv"
nom_fichier_edges_raw = "data/raw/edges.csv"

nom_fichier_nodes_parquet = "data/bronze/nodes.parquet"
nom_fichier_edges_parquet = "data/bronze/edges.parquet"

CHUNK_SIZE = 100000


def csv_to_parquet(csv_file, parquet_file):
    """
    Convertit un fichier CSV en un fichier parquet par lots
    """
    print(f"Début de la conversion de {csv_file} vers {parquet_file}...")

    # Création de l'itérateur pour lire le CSV par lots
    csv_iterator = pd.read_csv(csv_file, chunksize=CHUNK_SIZE)

    try:
        first_chunk = next(csv_iterator)
    except StopIteration:
        print(f"Le fichier {csv_file} est vide. Conversion annulée.")
        return

    table = pa.Table.from_pandas(first_chunk, preserve_index=False)

    writer = pq.ParquetWriter(parquet_file, table.schema)
    writer.write_table(table)

    total_rows = len(first_chunk)
    print(f"Batch initial traité. Total écrit : {total_rows:,} lignes")

    for batch in csv_iterator:
        table = pa.Table.from_pandas(batch, preserve_index=False)
        writer.write_table(table)

        total_rows += len(batch)
        print(f"Batch traité. Total écrit : {total_rows:,} lignes")

    writer.close()

    print(
        f"\nConversion de {csv_file} terminée avec succès! Total: {total_rows:,} lignes."
    )


if __name__ == "__main__":
    # TODO: vérifier que les fichiers csv existent

    # "parquettisation" du fichier data/raw/nodes.csv
    csv_to_parquet(nom_fichier_nodes_raw, nom_fichier_nodes_parquet)

    # "parquettisation" du fichier data/raw/edges.csv
    csv_to_parquet(nom_fichier_edges_raw, nom_fichier_edges_parquet)

    print("Les fichiers parquet ont été 'stratifiés'")
