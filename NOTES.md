## Regenerating the seed data

The committed `postgres-data/` directory is what makes `docker compose up` zero-setup. To rebuild
it after changing the schema or the generator:

```sh
# 1. Edit db/init/00_schema.sql and/or db/generate_seed.py, then regenerate:
python3 db/generate_seed.py > db/init/10_seed.sql

# 2. Wipe the committed data directory and re-bootstrap:
docker compose down
rm -rf postgres-data/* postgres-data/.[!.]*
docker compose up --detach
docker compose logs --follow postgres    # Wait for "ready to accept connections"
docker compose down

# 3. Commit the rebuilt postgres-data/ alongside your schema/seed changes.
```

The generator uses a fixed random seed, so step 1 produces identical SQL as long as the generator
source is unchanged. The postgres container runs as your host UID/GID (see `.env`), so everything
in `postgres-data/` is owned by you and can be removed without a root shell.

