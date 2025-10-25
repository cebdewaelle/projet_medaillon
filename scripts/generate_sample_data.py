import random
import pandas as pd


MAX_NODES = 1000000
MAX_EDGES = 5000000
BATCH_SIZE = 100000

labels_possibles = ["Person", "Org", "Paper"]

nom_fichier_nodes = "data/raw/nodes.csv"
nom_fichier_edges = "data/raw/edges.csv"


def generer_nodes():
    """Génèration du fichier CSV de nœuds (id, label, name)"""

    # Création du fichier de noeuds avec l'entête (nodes.csv)
    pd.DataFrame(columns=["id", "label", "name"]).to_csv(nom_fichier_nodes, index=False)

    for start in range(0, MAX_NODES, BATCH_SIZE):
        end = min(start + BATCH_SIZE, MAX_NODES)
        batch = [
            {"id": i, "label": random.choice(labels_possibles), "name": f"name_{i}"}
            for i in range(start, end)
        ]
        df = pd.DataFrame(batch)
        df.to_csv(nom_fichier_nodes, mode="a", header=False, index=False)

    print(f"Fichier {nom_fichier_nodes} généré ({MAX_NODES:,} lignes)\n")


def generer_edges():
    """Génération du fichier CSV de relations (src, dst, rel)"""
    # Création du fichier de relations avec l'entête (edges.csv)
    pd.DataFrame(columns=["src", "dst", "rel"]).to_csv(nom_fichier_edges, index=False)

    nb_edges_total = 0
    print(f"Génération de {MAX_EDGES:,} edges par batches de {BATCH_SIZE:,}...")

    while nb_edges_total < MAX_EDGES:
        # Calculer la taille du batch actuel (pour le dernier batch)
        current_batch_size = min(BATCH_SIZE, MAX_EDGES - nb_edges_total)

        # Générer le batch de données
        batch = {
            "src": [
                random.randint(0, MAX_NODES - 1) for _ in range(current_batch_size)
            ],
            "dst": [
                random.randint(0, MAX_NODES - 1) for _ in range(current_batch_size)
            ],
            "rel": ["REL"] * current_batch_size,
        }

        # Créer le DataFrame du batch
        df = pd.DataFrame(batch)

        # Ajouter au fichier (mode append)
        df.to_csv(nom_fichier_edges, mode="a", header=False, index=False)

        # Incrémenter le compteur
        nb_edges_total += current_batch_size

    print(f"\n\nFichier {nom_fichier_edges} généré avec succès!")
    print(f"Total: {MAX_EDGES:,} edges générés\n")


if __name__ == "__main__":
    generer_nodes()
    generer_edges()
    print("Jeu de données complet généré")


# def generer_edges_parfaits():
#     """Génèration du fichier CSV de relations (src, dst, rel)"""

#     # Crée le fichier de relations avec l'entête
#     pd.DataFrame(columns=["src", "dst", "rel"]).to_csv(nom_fichier_edges, index=False)

#     edges_generees = 0
#     paires_rencontrees = set()  # pour éviter les inversions entre batchs récents

#     while edges_generees < MAX_EDGES:
#         edges_restantes = min(BATCH_SIZE, MAX_EDGES - edges_generees)
#         edges_set = set()

#         while len(edges_set) < edges_restantes:
#             src = random.randint(0, MAX_NODES - 1)
#             dst = random.randint(0, MAX_NODES - 1)
#             if src == dst:
#                 continue

#             # Tri pour imposer un ordre et interdire les inverses
#             pair = tuple(sorted((src, dst)))

#             if pair not in seen_pairs:
#                 edges_set.add(pair)
#                 seen_pairs.add(pair)

#         df = pd.DataFrame([{"src": s, "dst": d, "rel": "REL"} for s, d in edges_set])
#         df.to_csv(nom_fichier_edges, mode="a", header=False, index=False)

#         print(
#             f"Batch {generated_edges:,}–{generated_edges + len(df) - 1:,} écrit "
#             f"({len(df):,} arêtes uniques)"
#         )
#         generated_edges += len(df)

#         # Pour ne pas exploser la mémoire : on garde un set raisonnable
#         # On supprime progressivement les anciennes relations
#         if len(seen_pairs) > 5_000_000:
#             seen_pairs = set(list(seen_pairs)[-2_000_000:])

#     print(
#         f"Fichier {nom_fichier_edges} généré ({MAX_EDGES:,} arêtes uniques non orientées)\n"
#     )
