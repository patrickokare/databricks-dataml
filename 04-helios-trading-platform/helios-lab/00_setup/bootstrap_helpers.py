# Databricks notebook source
# MAGIC %md
# MAGIC # Helios Trading Corporation: Bootstrap Helpers
# MAGIC
# MAGIC Shared, idempotent functions every lab leans on so the same plumbing is not re-taught in each notebook.
# MAGIC `%run` this near the top of a lab to bring the helpers into scope. Like the generator, it only defines
# MAGIC functions, so `%run` has no side effects.
# MAGIC
# MAGIC What lives here:
# MAGIC - `helios_identity()`, derive the student catalog from `current_user()` and create catalog, schemas and
# MAGIC   the landing Volume. The canonical identity cell, in one call.
# MAGIC - `ensure_landing(catalog, up_to_batch)`, make sure batches 1..N are landed, idempotently.
# MAGIC - `reset_schema(catalog, schema)`, drop and recreate a layer so a bootstrap starts from a known state.
# MAGIC - `check(label, expected, actual)`, the shared self check printer the acceptance criteria use.
# MAGIC - `show_landing(catalog)`, list what is in the landing Volume.
# MAGIC
# MAGIC Pattern for a lab bootstrap cell (Section 3 onward):
# MAGIC ```python
# MAGIC %run ../00_setup/data_generator
# MAGIC ```
# MAGIC ```python
# MAGIC %run ../00_setup/bootstrap_helpers
# MAGIC ```
# MAGIC ```python
# MAGIC catalog = helios_identity()
# MAGIC ensure_landing(catalog, up_to_batch=1)
# MAGIC ```
# MAGIC Both `%run`s land their functions in the same Python namespace, so `ensure_landing` can call the
# MAGIC generator's `generate_batch` directly.

# COMMAND ----------

LAYERS = ["helios_raw", "helios_bronze", "helios_silver", "helios_gold", "helios_semantic"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Identity
# MAGIC
# MAGIC Derives the catalog from the signed in user, with no manual widgets, so every student lands in their own
# MAGIC isolated namespace and the notebooks are identical for everyone. On Free Edition the student owns their
# MAGIC metastore, so creation works; on Vocareum the provided catalog is used the same way.

# COMMAND ----------

def helios_identity(verbose: bool = True) -> str:
    """Create (if needed) and select the student's catalog, schemas and landing Volume. Returns the catalog."""
    username = spark.sql("SELECT current_user()").first()[0]
    user_key = username.split("@")[0].replace(".", "_").replace("-", "_").lower()
    catalog = f"labs_{user_key}"

    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
    spark.sql(f"USE CATALOG {catalog}")
    for layer in LAYERS:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{layer}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.helios_raw.helios_landing")

    if verbose:
        print(f"Signed in as {username}")
        print(f"Catalog      {catalog}  (in use)")
        print(f"Schemas      {', '.join(LAYERS)}")
        print(f"Landing      /Volumes/{catalog}/helios_raw/helios_landing")
    return catalog

# COMMAND ----------

# MAGIC %md
# MAGIC ## Landing
# MAGIC
# MAGIC `ensure_landing` lands every batch up to the one the lab needs. Because `generate_batch` overwrites each
# MAGIC batch folder, calling this repeatedly is safe and stays well under the two minute bootstrap budget.

# COMMAND ----------

def ensure_landing(catalog: str, up_to_batch: int = 1, force: bool = False) -> dict:
    """Land batches 1..up_to_batch idempotently. Requires the generator to have been %run in this notebook.

    A batch already present in the Volume is skipped, because re-landing rewrites the files with new names and
    Auto Loader would treat them as new and re-ingest them, duplicating Bronze on every bootstrap rerun. Pass
    force=True to re-land regardless. Returns the counts from the last batch actually landed (empty if all
    were skipped)."""
    if "generate_batch" not in globals():
        raise RuntimeError(
            "generate_batch is not defined. Add `%run ../00_setup/data_generator` above this cell."
        )
    root = f"/Volumes/{catalog}/helios_raw/helios_landing"
    last = {}
    for b in range(1, up_to_batch + 1):
        already = False
        if not force:
            try:
                # order_lines is the hero fact, landed for every batch, so its folder is the reliable probe.
                dbutils.fs.ls(f"{root}/order_lines/batch_{b}")
                already = True
            except Exception:  # noqa: BLE001  (path does not exist yet)
                already = False
        if already:
            print(f"Batch {b} already landed, skipping")
        else:
            last = generate_batch(catalog, batch=b)
    return last


def show_landing(catalog: str) -> None:
    """Print the top level of the landing Volume, so you can see what has arrived."""
    root = f"/Volumes/{catalog}/helios_raw/helios_landing"
    try:
        for f in dbutils.fs.ls(root):
            print(f.name)
    except Exception as e:  # noqa: BLE001
        print(f"Landing zone {root} is empty or missing ({e})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Layer reset
# MAGIC
# MAGIC A bootstrap restores the exact prior state a lab needs. `reset_schema` gives a clean layer to rebuild
# MAGIC into, so reruns never inherit half built tables. Later sections also prebake upstream Delta state and
# MAGIC restore it here; that hook is added when the first downstream lab needs it.

# COMMAND ----------

def reset_schema(catalog: str, schema: str) -> None:
    """Drop and recreate one schema in the student catalog. Use to start a layer from scratch."""
    if schema not in LAYERS:
        raise ValueError(f"{schema} is not a Helios layer. Expected one of {LAYERS}.")
    spark.sql(f"DROP SCHEMA IF EXISTS {catalog}.{schema} CASCADE")
    spark.sql(f"CREATE SCHEMA {catalog}.{schema}")
    print(f"Reset {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Self check printer
# MAGIC
# MAGIC Every acceptance criterion in every lab prints expected versus actual through this one helper, so a green
# MAGIC PASS means the same thing everywhere. It raises on failure when `hard=True`, which is what a job run wants.

# COMMAND ----------

def check(label: str, expected, actual, hard: bool = False) -> bool:
    """Print expected vs actual with a PASS/FAIL. Optionally raise on failure."""
    ok = expected == actual
    mark = "✅ PASS" if ok else "⛔ FAIL"
    print(f"{mark} {label}\n        expected: {expected!r}\n        actual:   {actual!r}")
    if not ok and hard:
        raise AssertionError(f"Check failed: {label} (expected {expected!r}, got {actual!r})")
    return ok


def check_true(label: str, actual: bool, hard: bool = False) -> bool:
    """Shorthand for a boolean acceptance criterion."""
    return check(label, True, bool(actual), hard=hard)