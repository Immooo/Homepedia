import os
from sqlalchemy import create_engine, MetaData

DB_FILE = os.path.join("data", "homepedia.db")

def main() -> None:
    engine = create_engine(f"sqlite:///{DB_FILE}")
    metadata = MetaData()
    metadata.reflect(bind=engine)

    # --- construction du .dot « à la main » ---
    lines = [
        "digraph ERD {",
        "  rankdir=LR;",
        "  node [shape=record, fontsize=10, fontname=Helvetica];",
    ]

    # 1. Noeuds = tables
    for table in metadata.sorted_tables:
        cols = "|".join(f"<{c.name}>{c.name}" for c in table.columns)
        lines.append(f'  {table.name} [label="{{{table.name}|{cols}}}"];')

    # 2. Arêtes = clés étrangères
    for table in metadata.sorted_tables:
        for fk in table.foreign_keys:
            src = f"{table.name}:{fk.parent.name}"
            dst = f"{fk.column.table.name}:{fk.column.name}"
            lines.append(f"  {src} -> {dst};")

    lines.append("}")

    os.makedirs("docs", exist_ok=True)
    dot_path = os.path.join("docs", "homepedia_erd.dot")
    with open(dot_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print(f"✅ Fichier DOT généré : {dot_path}")

if __name__ == "__main__":
    main()
