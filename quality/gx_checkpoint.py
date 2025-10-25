import great_expectations as gx
import pandas as pd


nom_fichier_nodes_parquet = "data/bronze/nodes.parquet"
nom_fichier_edges_parquet = "data/bronze/edges.parquet"

NODES_COL_UNIQUE = ["id"]
EDGES_COL_NOT_NULL = ["src", "dst"]


def verifier_unique(context, batch_definition, df, cols):
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

    for col in cols:
        expectation = gx.expectations.ExpectColumnValuesToBeUnique(
            column=col, severity="critical"
        )

        validation_result = batch.validate(expectation)
        print(validation_result)


def verifier_not_null(context, batch_definition, df, cols):
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

    for col in cols:
        expectation = gx.expectations.ExpectColumnValuesToNotBeNull(
            column=col, severity="critical"
        )

        validation_result = batch.validate(expectation)
        print(validation_result)


if __name__ == "__main__":
    # TODO: vérifier que les fichiers parquet existent
    context = gx.get_context()

    data_source = context.data_sources.add_pandas("pandas")
    data_asset = data_source.add_dataframe_asset(name="pd dataframe asset")

    batch_definition = data_asset.add_batch_definition_whole_dataframe(
        "batch definition"
    )

    # Nodes
    df = pd.read_parquet(nom_fichier_nodes_parquet)
    verifier_unique(context, batch_definition, df, NODES_COL_UNIQUE)

    # Edges
    df = pd.read_parquet(nom_fichier_edges_parquet)
    verifier_not_null(context, batch_definition, df, EDGES_COL_NOT_NULL)

    print("Les fichiers parquet ont été 'vérifiés'")
