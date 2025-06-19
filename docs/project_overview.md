## Database Schema

### Table `transactions`

| Colonne                 | Type             | Description                             |
|-------------------------|------------------|-----------------------------------------|
| `id`                    | SERIAL PRIMARY KEY | Identifiant interne                   |
| `date_mutation`         | DATE             | Date de la transaction                  |
| `nature_mutation`       | VARCHAR(50)      | Type de mutation (ex. Vente)            |
| `valeur_fonciere`       | NUMERIC(12,2)    | Montant de la transaction (€)           |
| `code_postal`           | VARCHAR(10)      | Code postal                             |
| `commune`               | VARCHAR(100)     | Nom de la commune                       |
| `type_local`            | VARCHAR(50)      | Type de logement (Maison, Appartement)  |
| `surface_reelle_bati`   | NUMERIC(10,2)    | Surface bâtie réelle (m²)               |
| `nombre_pieces_principales` | INTEGER      | Nombre de pièces principales            |