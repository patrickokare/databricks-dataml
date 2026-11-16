# Databricks notebook source
# MAGIC %md
# MAGIC # Helios Trading Corporation: Synthetic Data Generator
# MAGIC
# MAGIC One deterministic, dependency light generator for the whole course. Fixed seed, parameterised by `batch`, so
# MAGIC successive drops demonstrate incremental loads, CDC order lifecycles, SCD Type 2 price and tier changes,
# MAGIC late arriving returns, and duplicate, late and dirty records. It writes raw source files into the landing
# MAGIC Volume `helios_raw.helios_landing`, laid out the way Auto Loader and Lakeflow expect.
# MAGIC
# MAGIC **This notebook only defines functions. It has no side effects on `%run`.** A lab calls
# MAGIC `generate_batch(catalog, batch=1)` when it wants data to land, so it is always safe to `%run` from a bootstrap
# MAGIC cell to bring the functions into scope.
# MAGIC
# MAGIC Why deterministic with a fixed seed: every student lands byte for byte the same data, so the self checks, the
# MAGIC reveal solutions and the scripts all line up. The whole dataset projects from one ground truth in dependency
# MAGIC order (dimensions, then prices, then orders, then lines, then returns and stock), so every foreign key
# MAGIC resolves by construction and only the deliberate, counted data quality injections break a link.
# MAGIC
# MAGIC ### The ten feeds, their shape, and how the medallion consumes them
# MAGIC The **shape of each feed decides how it is written downstream**, the most important thing to know about a
# MAGIC source. A full snapshot is overwritten (self healing, captures removals); an append, CDC or effective dated
# MAGIC feed is merged or upserted.
# MAGIC
# MAGIC | Feed | Landing path (format) | Shape | How Silver / Gold writes it |
# MAGIC |---|---|---|---|
# MAGIC | order_lines | `order_lines/batch_<n>/` (JSON) | append stream, high volume, the hero fact, duplicates and late records | stream to Silver, **upsert** on `order_line_id`; Gold fact upserts on it |
# MAGIC | orders | `orders/batch_<n>/` (JSON) | CDC change records (INSERT / UPDATE / DELETE, ordered by `change_seq`), the order lifecycle | **`DeltaTable.merge`**: latest state per id, soft delete |
# MAGIC | returns | `returns/batch_<n>/` (JSON) | append, late arriving (from batch 2), points back to a line | **upsert** on `return_id`; Gold `fact_returns` |
# MAGIC | products | `products/batch_<n>/` (Parquet) | reference list, landed on batch 1 | **overwrite** |
# MAGIC | categories | `categories/batch_<n>/` (Parquet) | reference taxonomy, landed on batch 1 | **overwrite** |
# MAGIC | suppliers | `suppliers/batch_<n>/` (JSON) | dimension, landed on batch 1 | **overwrite** |
# MAGIC | warehouses | `warehouses/batch_<n>/` (JSON) | the depot dimension, landed on batch 1 | **overwrite** |
# MAGIC | customers | `customers/snapshot_<date>/` (Parquet) | daily full snapshot, tier changes over time | **overwrite** latest snapshot; **SCD Type 2** for tier |
# MAGIC | price_list | `price_list/batch_<n>/` (CSV) | **effective dated** price and cost (SCD Type 2 source) | two step **SCD2 MERGE** |
# MAGIC | inventory | `inventory/batch_<n>/` (JSON) | append, stock movements derived from the actual sales | **upsert**; running `on_hand` per (depot, product) |
# MAGIC
# MAGIC See `02_design/2_2_source_feeds.md` for the source feeds and `02_design/2_4_lakehouse_design.md` for the medallion design these files implement.

# COMMAND ----------

import datetime as dt
import random
import hashlib
from decimal import Decimal
from functools import lru_cache
from collections import defaultdict, Counter

from pyspark.sql import types as T

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fixed parameters
# MAGIC
# MAGIC Scaled to read like a real trading house: about 50,000 to 71,000 order lines per batch (around 293,000 clean
# MAGIC across the five batches, batch three highest because the Ares incident adds a buying spike), so trends
# MAGIC survive aggregation, while the reference dimensions stay bounded (6 depots, 7 categories, 80 suppliers, 150
# MAGIC products, 300 customers). Demand is a deterministic signal per depot, category and day (a depot fingerprint
# MAGIC times a Titan growth trend, a seasonal cycle, a weekday factor and a seeded wobble), never live randomness.
# MAGIC About 300k rows is tiny for Delta, so the bootstrap restore and the Free Edition quotas stay comfortable.

# COMMAND ----------

SEED = 7331
BASE_DATE = dt.date(2257, 3, 1)
N_BATCHES = 5

# ----- bounded scale -----
N_SUPPLIERS = 80
N_PRODUCTS = 150
N_CUSTOMERS = 300
AVG_LINES_PER_ORDER = 3

# ----- enum domains (match the source feeds design) -----
CATEGORIES = [  # (id, name, department)
    ("CAT-1", "PROPULSION", "PROPULSION_AND_POWER"),
    ("CAT-2", "HULL_AND_STRUCTURE", "STRUCTURE"),
    ("CAT-3", "LIFE_SUPPORT", "HABITAT"),
    ("CAT-4", "POWER", "PROPULSION_AND_POWER"),
    ("CAT-5", "NAVIGATION", "AVIONICS"),
    ("CAT-6", "THERMAL", "HABITAT"),
    ("CAT-7", "CONSUMABLES", "CONSUMABLES"),
]
CATEGORY_IDS = [c[0] for c in CATEGORIES]
CUSTOMER_TYPES = ["FREIGHT_FLEET", "MINER", "LINER", "RESEARCH", "PATROL", "INDEPENDENT", "COLONIAL"]
TIERS = ["CHARTER", "TRADE", "DRIFTER"]
CHANNELS = ["CONSOLE", "API", "COUNTER"]
# How a customer orders tracks their tier: big contracted fleets reorder by API, regular businesses use the
# self-service console, walk-in independents buy at the counter. Weights over CHANNELS order [CONSOLE, API,
# COUNTER]; a deterministic per-batch wobble (in _channel_weights) shifts the mix a little each batch.
TIER_CHANNEL_WEIGHTS = {
    "CHARTER": [0.30, 0.60, 0.10],   # API led (automated procurement)
    "TRADE":   [0.55, 0.25, 0.20],   # CONSOLE led (self service)
    "DRIFTER": [0.30, 0.10, 0.60],   # COUNTER led (walk in)
}
ORDER_STATUSES = ["PLACED", "PAID", "PICKED", "SHIPPED", "DELIVERED", "BACKORDERED", "CANCELLED", "RETURNED"]
REGIONS = ["INNER", "BELT", "OUTER"]
RETURN_REASONS = ["FAULTY", "WRONG_PART", "NOT_NEEDED", "DAMAGED_IN_TRANSIT"]
HAZARD_CLASSES = [None, "FLAMMABLE", "CRYOGENIC", "RADIOACTIVE"]
CURRENCIES = ["CREDITS"]

# ----- warehouses (the 6 depots) -----
WAREHOUSES = [  # (id, name, body, region, uplink_reliability)
    ("DEP-01", "Helios Prime", "Luna", "INNER", 0.99),
    ("DEP-02", "Luna Hub", "Luna", "INNER", 0.98),
    ("DEP-03", "Ares Depot", "Mars", "INNER", 0.95),
    ("DEP-04", "Ceres Exchange", "Belt", "BELT", 0.90),
    ("DEP-05", "Europa Outpost", "Europa", "OUTER", 0.80),
    ("DEP-06", "Titan Yard", "Titan", "OUTER", 0.85),
]
DEPOT_IDS = [w[0] for w in WAREHOUSES]

# Per depot, expected order LINES per day (the volume fingerprint). Sum ~= 54,000.
DEPOT_BASE_LINES = {
    "DEP-01": 11000, "DEP-02": 16000, "DEP-03": 12000,
    "DEP-04": 8000, "DEP-05": 3500, "DEP-06": 3500,
}
# What each customer SEGMENT buys: a category weight vector (over CATEGORY_IDS order: propulsion, hull,
# life support, power, navigation, thermal, consumables). This is what makes a miner buy differently from a
# passenger liner. Everyone buys some consumables (the volume baseline).
SEGMENT_CAT_WEIGHTS = {
    "FREIGHT_FLEET": [0.22, 0.16, 0.06, 0.18, 0.06, 0.07, 0.25],   # cargo ships: propulsion, power, hull
    "MINER":         [0.20, 0.14, 0.05, 0.18, 0.05, 0.13, 0.25],   # mining rigs: propulsion, power, thermal
    "LINER":         [0.06, 0.06, 0.24, 0.06, 0.10, 0.08, 0.40],   # passenger liners: life support, consumables
    "RESEARCH":      [0.07, 0.08, 0.20, 0.08, 0.24, 0.13, 0.20],   # science: navigation, life support
    "PATROL":        [0.18, 0.14, 0.07, 0.10, 0.22, 0.06, 0.23],   # patrol craft: navigation, propulsion
    "INDEPENDENT":   [0.14, 0.12, 0.13, 0.12, 0.11, 0.09, 0.29],   # broad mix
    "COLONIAL":      [0.06, 0.10, 0.22, 0.08, 0.08, 0.16, 0.30],   # colony supply: life support, thermal
}

# Each depot's customer base skews to segments that fit its character, so the depot demand fingerprint
# emerges from WHO is homed there (Ares is miners and freighters, Europa is research, and so on).
DEPOT_SEGMENTS = {
    "DEP-01": [("FREIGHT_FLEET", 1), ("MINER", 1), ("LINER", 1), ("RESEARCH", 1),
               ("PATROL", 1), ("INDEPENDENT", 1), ("COLONIAL", 1)],                 # broad baseline
    "DEP-02": [("LINER", 4), ("COLONIAL", 2), ("INDEPENDENT", 2), ("RESEARCH", 1)],  # commuter trade
    "DEP-03": [("FREIGHT_FLEET", 4), ("MINER", 3), ("PATROL", 1), ("INDEPENDENT", 1)],  # heavy industry
    "DEP-04": [("MINER", 5), ("FREIGHT_FLEET", 2), ("INDEPENDENT", 1)],              # mining
    "DEP-05": [("RESEARCH", 4), ("PATROL", 2), ("INDEPENDENT", 1)],                  # deep space science
    "DEP-06": [("COLONIAL", 4), ("INDEPENDENT", 2), ("LINER", 1), ("MINER", 1)],     # frontier colony
}

# ----- the incident -----
INCIDENT_DEPOT = "DEP-03"            # Ares
INCIDENT_SUPPLIER = "SUP-11"
INCIDENT_DAYS = {2}                  # day index 2 == 2257-03-03 == batch 3
INCIDENT_SPIKE_LINES = 6000          # extra incident-product lines at Ares on the incident day

# ----- messiness fractions -----
DUP_FRAC = 0.02
LATE_FRAC = 0.015
DIRTY_FRAC = 0.01
ORPHAN_RETURN_FRAC = 0.03            # of returns

# ----- RNG channel discipline (integer-seeded, PYTHONHASHSEED independent) -----
(_CH_SUP, _CH_PROD, _CH_CUST, _CH_PRICE, _CH_ORDERS, _CH_LINES,
 _CH_RETURNS, _CH_INV, _CH_MESS) = range(1, 10)


def _rng(channel, batch=0):
    return random.Random(SEED + channel * 1_000_000 + batch * 1000)


def _day(batch):
    return BASE_DATE + dt.timedelta(days=batch - 1)


def _dayidx(date):
    return (date - BASE_DATE).days

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spark schemas
# MAGIC
# MAGIC Explicit schemas pin types and column order. JSON and CSV cannot carry real types, so those feeds land as
# MAGIC text shaped data and Silver casts them; Parquet keeps its types. That mismatch is deliberate, it is what the
# MAGIC medallion layers exist to resolve.

# COMMAND ----------

ORDER_LINES_FIELDS = [
    T.StructField("order_line_id", T.StringType()),
    T.StructField("order_id", T.StringType()),
    T.StructField("product_id", T.StringType()),
    T.StructField("customer_id", T.StringType()),
    T.StructField("warehouse_id", T.StringType()),
    T.StructField("quantity", T.IntegerType()),
    T.StructField("line_ts", T.TimestampType()),
]
ORDER_LINES_SCHEMA = T.StructType(ORDER_LINES_FIELDS)

ORDERS_SCHEMA = T.StructType([
    T.StructField("order_id", T.StringType()),
    T.StructField("customer_id", T.StringType()),
    T.StructField("warehouse_id", T.StringType()),
    T.StructField("channel", T.StringType()),
    T.StructField("order_ts", T.TimestampType()),
    T.StructField("status", T.StringType()),
    T.StructField("op", T.StringType()),
    T.StructField("change_ts", T.TimestampType()),
    T.StructField("change_seq", T.LongType()),
])

RETURNS_SCHEMA = T.StructType([
    T.StructField("return_id", T.StringType()),
    T.StructField("order_line_id", T.StringType()),
    T.StructField("return_ts", T.TimestampType()),
    T.StructField("quantity", T.IntegerType()),
    T.StructField("reason", T.StringType()),
])

PRODUCTS_SCHEMA = T.StructType([
    T.StructField("product_id", T.StringType()),
    T.StructField("sku", T.StringType()),
    T.StructField("product_name", T.StringType()),
    T.StructField("category_id", T.StringType()),
    T.StructField("supplier_id", T.StringType()),
    T.StructField("mass_kg", T.DoubleType()),
    T.StructField("hazard_class", T.StringType()),
    T.StructField("active", T.BooleanType()),
])

CATEGORIES_SCHEMA = T.StructType([
    T.StructField("category_id", T.StringType()),
    T.StructField("category_name", T.StringType()),
    T.StructField("department", T.StringType()),
])

SUPPLIERS_SCHEMA = T.StructType([
    T.StructField("supplier_id", T.StringType()),
    T.StructField("supplier_name", T.StringType()),
    T.StructField("home_region", T.StringType()),
    T.StructField("active", T.BooleanType()),
])

CUSTOMERS_SCHEMA = T.StructType([
    T.StructField("customer_id", T.StringType()),
    T.StructField("customer_name", T.StringType()),
    T.StructField("customer_type", T.StringType()),
    T.StructField("tier", T.StringType()),
    T.StructField("home_warehouse_id", T.StringType()),
    T.StructField("signup_date", T.DateType()),
    T.StructField("snapshot_date", T.DateType()),
])

WAREHOUSES_SCHEMA = T.StructType([
    T.StructField("warehouse_id", T.StringType()),
    T.StructField("warehouse_name", T.StringType()),
    T.StructField("body", T.StringType()),
    T.StructField("region", T.StringType()),
    T.StructField("uplink_reliability", T.DoubleType()),
])

PRICE_LIST_SCHEMA = T.StructType([
    T.StructField("product_id", T.StringType()),
    T.StructField("supplier_id", T.StringType()),
    T.StructField("unit_price", T.DecimalType(12, 2)),
    T.StructField("unit_cost", T.DecimalType(12, 2)),
    T.StructField("currency", T.StringType()),
    T.StructField("effective_from", T.DateType()),
    T.StructField("effective_to", T.DateType()),
    T.StructField("is_current", T.BooleanType()),
])

INVENTORY_SCHEMA = T.StructType([
    T.StructField("warehouse_id", T.StringType()),
    T.StructField("product_id", T.StringType()),
    T.StructField("movement_ts", T.TimestampType()),
    T.StructField("delta", T.IntegerType()),
    T.StructField("on_hand", T.IntegerType()),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## The stable universe
# MAGIC
# MAGIC Everything projects from one deterministic ground truth, built in dependency order so every foreign key
# MAGIC resolves: the dimensions (warehouses, categories, suppliers, products, customers) first, then the effective
# MAGIC dated `price_list`, then `orders` (each on a real customer and depot) with a monotone lifecycle and their
# MAGIC `order_lines`, then `returns` (post delivery, dated into a later batch), then `inventory` (stock movements
# MAGIC derived from the actual fulfilled sales plus restocks, so `on_hand` is a true running balance). The Ares
# MAGIC (`DEP-03`) pricing incident on the supplier `SUP-11` propulsion part on batch three is woven through: a
# MAGIC buying spike in the lines, a cancellation wave in the orders, a later return wave, and a stockout in stock.

# COMMAND ----------

# ======================================================================
# Dimensions (generated first so every foreign key resolves by construction)
# ======================================================================
_FIRST = ["Anya", "Ben", "Cass", "Dmitri", "Elena", "Faraz", "Greta", "Hiro", "Imani", "Jonas",
          "Kira", "Liang", "Mara", "Niko", "Omar", "Priya", "Quinn", "Rosa", "Sven", "Tara"]
_LAST = ["Reyes", "Okonkwo", "Lindgren", "Vex", "Marsh", "Idris", "Holm", "Tanaka", "Cole", "Pike",
         "Sato", "Novak", "Adeyemi", "Bauer", "Costa", "Diaz", "Engel", "Fischer"]
_FLEET = ["Orion", "Vega", "Lyra", "Draco", "Cygnus", "Phoenix", "Hydra", "Lupus"]


@lru_cache(maxsize=None)
def warehouses_base():
    return [{"warehouse_id": w[0], "warehouse_name": w[1], "body": w[2], "region": w[3],
             "uplink_reliability": w[4]} for w in WAREHOUSES]


@lru_cache(maxsize=None)
def categories_base():
    return [{"category_id": c[0], "category_name": c[1], "department": c[2]} for c in CATEGORIES]


@lru_cache(maxsize=None)
def suppliers_base():
    r = _rng(_CH_SUP)
    out = []
    for i in range(1, N_SUPPLIERS + 1):
        out.append({"supplier_id": f"SUP-{i:02d}", "supplier_name": f"{r.choice(_FLEET)} Supply {i:02d}",
                    "home_region": r.choice(REGIONS), "active": (i % 13 != 0)})
    return out


@lru_cache(maxsize=None)
def products_base():
    """150 products. Each belongs to a category and a supplier. One propulsion product from the incident
    supplier SUP-11 is the incident product."""
    r = _rng(_CH_PROD)
    out = []
    for i in range(1, N_PRODUCTS + 1):
        cat = CATEGORIES[i % len(CATEGORIES)]           # spread across the 7 categories
        sup = f"SUP-{(i % N_SUPPLIERS) + 1:02d}"
        out.append({"product_id": f"PRD-{i:03d}", "sku": f"{cat[1][:3]}-{1000 + i}",
                    "product_name": f"{cat[1].title().replace('_', ' ')} unit {i:03d}",
                    "category_id": cat[0], "supplier_id": sup,
                    "mass_kg": round(r.uniform(0.2, 1800.0), 2),
                    "hazard_class": r.choice(HAZARD_CLASSES), "active": (i % 17 != 0),
                    "_cat_name": cat[1], "_i": i})
    # designate the incident product: a PROPULSION product, force its supplier to SUP-11.
    for p in out:
        if p["_cat_name"] == "PROPULSION":
            p["supplier_id"] = INCIDENT_SUPPLIER
            p["_incident"] = True
            break
    return out


@lru_cache(maxsize=None)
def _incident_product():
    for p in products_base():
        if p.get("_incident"):
            return p["product_id"]
    raise RuntimeError("no incident product")


@lru_cache(maxsize=None)
def _products_by_category():
    by = defaultdict(list)
    for p in products_base():
        by[p["category_id"]].append(p["product_id"])
    return {k: tuple(v) for k, v in by.items()}


def _base_tier(i):
    # skewed: more DRIFTER/TRADE than CHARTER
    return TIERS[0] if i % 5 == 0 else (TIERS[1] if i % 2 == 0 else TIERS[2])


def _tier_promotes_at(i):
    """A deterministic subset of customers are promoted one tier up at a fixed batch (the tier SCD2 story)."""
    if i % 11 == 0:
        return 3 + (i // 11) % 3      # promoted at batch 3, 4 or 5
    return None


def _tier_at(i, batch):
    base = _base_tier(i)
    pb = _tier_promotes_at(i)
    if pb is not None and batch >= pb:
        # promote one step toward CHARTER (DRIFTER->TRADE->CHARTER)
        idx = TIERS.index(base)
        return TIERS[max(0, idx - 1)]
    return base


@lru_cache(maxsize=None)
def customers_base():
    r = _rng(_CH_CUST)
    out = []
    for i in range(1, N_CUSTOMERS + 1):
        dep = DEPOT_IDS[i % len(DEPOT_IDS)]
        signup = BASE_DATE - dt.timedelta(days=r.randint(30, 1500))
        segs, ws = zip(*DEPOT_SEGMENTS[dep])          # segment fits the home depot's character
        ctype = r.choices(segs, weights=ws)[0]
        out.append({"customer_id": f"CUST-{i:04d}",
                    "customer_name": f"{_FIRST[(i - 1) % len(_FIRST)]} {_LAST[(i * 5) % len(_LAST)]}",
                    "customer_type": ctype,
                    "home_warehouse_id": dep, "signup_date": signup, "_i": i})
    return out


@lru_cache(maxsize=None)
def _customers_by_depot():
    by = defaultdict(list)
    for c in customers_base():
        by[c["home_warehouse_id"]].append(c)
    return {k: tuple(v) for k, v in by.items()}


# ======================================================================
# price_list: SCD Type 2 source (price + cost), effective dated, replays changes per batch
# ======================================================================
# Category price bands (lo, hi, cost_factor): consumables are cheap and thin margin (the volume driver),
# propulsion and power are expensive and fat margin (the value drivers). cost = price * cost_factor.
CATEGORY_PRICE = {
    "CONSUMABLES":        (5,    80,    Decimal("0.85")),
    "LIFE_SUPPORT":       (150,  1200,  Decimal("0.70")),
    "THERMAL":            (150,  1500,  Decimal("0.70")),
    "HULL_AND_STRUCTURE": (200,  2500,  Decimal("0.68")),
    "NAVIGATION":         (400,  4000,  Decimal("0.64")),
    "POWER":              (2000, 18000, Decimal("0.56")),
    "PROPULSION":         (3000, 25000, Decimal("0.55")),
}


def _price_cost(p):
    """Deterministic price and supplier cost for a product, set by its category band (no RNG, stable per id)."""
    lo, hi, cost_factor = CATEGORY_PRICE[p["_cat_name"]]
    frac = ((p["_i"] * 2654435761) % 997) / 997.0          # a stable spread within the band
    price = Decimal(int(lo + frac * (hi - lo))) + Decimal("0.99")
    cost = (price * cost_factor).quantize(Decimal("0.01"))
    return price, cost


def _price_changes_at(i, batch):
    return batch >= 2 and ((i + batch) % 4 == 0)


def gen_price_list(batch):
    """Full effective-dated extract known at this batch. One current row per product. The incident product
    carries a deliberate misprice version effective on the incident day, corrected the next day."""
    out = []
    inc_pid = _incident_product()
    for p in products_base():
        i = p["_i"]
        pid, sup = p["product_id"], p["supplier_id"]
        price, cost = _price_cost(p)
        eff_from = BASE_DATE
        for b in range(2, batch + 1):
            incident_misprice = (pid == inc_pid and b == 3)        # the Ares mispricing on the incident day
            incident_correct = (pid == inc_pid and b == 4)         # caught and corrected the next day
            changed = _price_changes_at(i, b) and pid != inc_pid   # the incident product only moves for the glitch
            if changed or incident_misprice or incident_correct:
                change_date = _day(b)
                out.append({"product_id": pid, "supplier_id": sup, "unit_price": price, "unit_cost": cost,
                            "currency": "CREDITS", "effective_from": eff_from, "effective_to": change_date,
                            "is_current": False})
                if incident_misprice:
                    price = (price * Decimal("0.45")).quantize(Decimal("0.01"))   # mispriced far too low
                elif incident_correct:
                    price = (price / Decimal("0.45")).quantize(Decimal("0.01"))   # restored to its real level
                else:
                    price = (price * Decimal("1.07")).quantize(Decimal("0.01"))
                    cost = (cost * Decimal("1.05")).quantize(Decimal("0.01"))
                eff_from = change_date
        out.append({"product_id": pid, "supplier_id": sup, "unit_price": price, "unit_cost": cost,
                    "currency": "CREDITS", "effective_from": eff_from, "effective_to": None, "is_current": True})
    return out


@lru_cache(maxsize=None)
def _price_history():
    """All price versions across the whole run, for the point-in-time pricing the lines/returns reference."""
    versions = defaultdict(list)
    full = gen_price_list(N_BATCHES)
    for row in full:
        versions[row["product_id"]].append((row["effective_from"], row["effective_to"], row["unit_price"], row["unit_cost"]))
    for pid in versions:
        versions[pid].sort(key=lambda x: x[0])
    return versions


def _price_at(pid, date):
    for ef, et, price, cost in _price_history()[pid]:
        if ef <= date and (et is None or date < et):
            return price, cost
    last = _price_history()[pid][-1]
    return last[2], last[3]


# ======================================================================
# orders + order_lines: the demand signal projected into orders, each with a monotone lifecycle and lines
# ======================================================================
def _trend(dayidx, depot):
    return 1.0 + 0.12 * dayidx if depot == "DEP-06" else 1.0      # Titan climbs


def _season(dayidx):
    return [1.00, 1.05, 1.12, 0.98, 1.03][dayidx % 5]


def _weekday(date):
    return 0.9 if date.weekday() >= 5 else 1.0     # mild, so the incident stays the dominant signal


def _noise(rng):
    return 1.0 + rng.uniform(-0.08, 0.08)


@lru_cache(maxsize=None)
def _channel_weights(tier, batch):
    """Tier channel weights with a deterministic per-batch wobble, so the channel mix moves a little each
    batch. Renormalised, seeded per (tier, batch), independent of any feed stream."""
    base = TIER_CHANNEL_WEIGHTS[tier]
    wr = random.Random(SEED + 70019 + batch * 131 + ord(tier[0]) * 17)
    wobbled = [w * wr.uniform(0.82, 1.18) for w in base]
    s = sum(wobbled)
    return tuple(w / s for w in wobbled)


def _channel(oidx, tier, batch):
    """Pick an order channel from the tier's wobbled weights, deterministically per order. Drawn from its own
    seeded source so it never shifts the order, line, return or inventory streams."""
    weights = _channel_weights(tier, batch)
    u = random.Random(SEED + 60013 * oidx + batch).random()
    acc = 0.0
    for ch, w in zip(CHANNELS, weights):
        acc += w
        if u < acc:
            return ch
    return CHANNELS[-1]


@lru_cache(maxsize=None)
def _all_orders():
    """Every order across the 5 days, each with its customer, depot, channel, placed day, a monotone
    lifecycle timeline, and its lines. Cached: per-batch slicers read from here."""
    orders = []
    oidx = 0
    lidx = 0
    inc_pid = _incident_product()
    for b in range(1, N_BATCHES + 1):
        date = _day(b)
        di = _dayidx(date)
        r = _rng(_CH_ORDERS, b)
        rl = _rng(_CH_LINES, b)
        for depot in DEPOT_IDS:
            base_lines = DEPOT_BASE_LINES[depot] * _trend(di, depot) * _season(di) * _weekday(date) * _noise(r)
            n_orders = max(1, round(base_lines / AVG_LINES_PER_ORDER))
            cust_pool = _customers_by_depot()[depot]
            for _ in range(n_orders):
                oidx += 1
                cust = r.choice(cust_pool)
                weights = SEGMENT_CAT_WEIGHTS[cust["customer_type"]]   # what THIS customer's segment buys
                secs = r.randint(0, 86399)
                order_ts = dt.datetime.combine(date, dt.time()) + dt.timedelta(seconds=secs)
                r.randrange(len(CHANNELS))                       # kept so the rest of the seeded stream is unchanged
                channel = _channel(oidx, _tier_at(cust["_i"], b), b)
                n_lines = max(1, min(6, round(rl.gauss(AVG_LINES_PER_ORDER, 1.2))))
                lines = []
                for _ln in range(n_lines):
                    cat = rl.choices(CATEGORY_IDS, weights=weights)[0]
                    pid = rl.choice(_products_by_category()[cat])
                    qty = _qty_for(cat, rl)
                    lidx += 1
                    lines.append({"order_line_id": f"OL-{b}-{lidx:06d}", "product_id": pid,
                                  "quantity": qty, "_cat": cat,
                                  "line_ts": min(order_ts + dt.timedelta(seconds=rl.randint(0, 120)), dt.datetime.combine(order_ts.date(), dt.time(23, 59, 59)))})
                orders.append(_make_order(oidx, cust, depot, channel, order_ts, b, di, lines, r))
            # incident: a buying spike of the incident product at Ares on the incident day
            if depot == INCIDENT_DEPOT and di in INCIDENT_DAYS:
                spike_orders = INCIDENT_SPIKE_LINES // 2
                for _ in range(spike_orders):
                    oidx += 1
                    cust = r.choice(cust_pool)
                    order_ts = dt.datetime.combine(date, dt.time()) + dt.timedelta(seconds=r.randint(0, 86399))
                    lines = []
                    for _ln in range(2):
                        qty = rl.randint(1, 3)
                        lidx += 1
                        lines.append({"order_line_id": f"OL-{b}-{lidx:06d}", "product_id": inc_pid,
                                      "quantity": qty, "_cat": "CAT-1",
                                      "line_ts": min(order_ts + dt.timedelta(seconds=rl.randint(0, 120)), dt.datetime.combine(order_ts.date(), dt.time(23, 59, 59)))})
                    channel = _channel(oidx, _tier_at(cust["_i"], b), b)
                    orders.append(_make_order(oidx, cust, depot, channel, order_ts, b, di, lines, r,
                                              incident=True))
    return orders


def _qty_for(cat, rng):
    if cat == "CAT-7":            # consumables: high volume
        return rng.randint(1, 25)
    if cat in ("CAT-1", "CAT-4"):  # propulsion / power: low count, high value
        return rng.randint(1, 2)
    return rng.randint(1, 5)


def _make_order(oidx, cust, depot, channel, order_ts, batch, di, lines, r, incident=False):
    region = next(w["region"] for w in warehouses_base() if w["warehouse_id"] == depot)
    fate = _order_fate(oidx, di, region, incident, r)
    timeline = _lifecycle(order_ts, region, fate, oidx, fast=incident)
    delivered_ts = next((ts for st, ts in timeline if st == "DELIVERED"), None)
    return {"order_id": f"ORD-{oidx:06d}", "customer_id": cust["customer_id"], "warehouse_id": depot,
            "channel": channel, "order_ts": order_ts, "_batch": batch, "_lines": lines,
            "_fate": fate, "_timeline": timeline, "_delivered_ts": delivered_ts,
            "_cust_i": cust["_i"], "_incident": incident}


def _order_fate(oidx, di, region, incident, r):
    """Deterministic terminal outcome of an order: normal delivery, a cancellation, a backorder-then-deliver,
    or delivered-then-returned. The incident raises cancellations at Ares on the incident day."""
    fr = random.Random(SEED + 4242 * oidx)
    cancel_p = 0.05
    backorder_p = 0.06 if region == "OUTER" else 0.03
    if incident:
        cancel_p = 0.35           # the cancellation wave once the glitch is caught
        backorder_p = 0.20
    x = fr.random()
    if x < cancel_p:
        return "CANCELLED"
    if x < cancel_p + backorder_p:
        return "BACKORDERED"
    return "NORMAL"


def _lifecycle(order_ts, region, fate, oidx, fast=False):
    """A forward-only timeline of (status, datetime) from PLACED. change_seq is the position. Far regions are
    slower; the incident orders are fast so the wave delivers and returns inside the five day window.
    Cancellations stop early; backorders insert a BACKORDERED step; normal orders reach DELIVERED."""
    fr = random.Random(SEED + 1313 * oidx)
    slow = 0 if fast else (1 if region == "OUTER" else 0)

    def hrs(lo, hi):
        return dt.timedelta(hours=fr.randint(lo, hi))

    t = order_ts
    steps = [("PLACED", t)]
    t = t + hrs(1, 6)
    if fate == "CANCELLED":
        if fr.random() < 0.5:
            steps.append(("PAID", t))
            t = t + hrs(2, 16)
        steps.append(("CANCELLED", t))
        return steps
    steps.append(("PAID", t))
    t = t + hrs(1, 10)
    if fate == "BACKORDERED":
        steps.append(("BACKORDERED", t))
        t = t + dt.timedelta(days=1 + slow) + hrs(0, 10)
    steps.append(("PICKED", t))
    t = t + hrs(2, 12)
    steps.append(("SHIPPED", t))
    t = t + dt.timedelta(days=slow) + hrs(4, 16)
    steps.append(("DELIVERED", t))
    return steps


def hash_str(s):
    """Stable cross-process small int from a string (no reliance on Python hash())."""
    import hashlib
    return int(hashlib.sha256(s.encode()).hexdigest()[:8], 16)


# ----- returns: seeded up front, post delivery, surface in the batch covering the return date -----
def _return_decision(order_line_id, delivered_ts, qty, incident):
    """Whether this line is ever returned, and if so when and how much. Deterministic per line. Dated after
    delivery so a return never precedes receipt. Incident lines return more, and sooner, so the wave is in window."""
    fr = random.Random(SEED + 97 * hash_str(order_line_id))
    p = 0.30 if incident else 0.06
    if fr.random() >= p:
        return None
    delay = fr.randint(1, 3) if incident else fr.randint(1, 6)
    return_ts = delivered_ts + dt.timedelta(days=delay, seconds=fr.randint(0, 80000))
    qret = qty if qty == 1 else fr.randint(1, qty)
    reason = RETURN_REASONS[fr.randrange(len(RETURN_REASONS))]
    return {"return_ts": return_ts, "quantity": qret, "reason": reason}


@lru_cache(maxsize=None)
def _all_returns():
    """Every return across the run, derived from delivered lines flagged returned. Each carries the parent
    order_id so we can drive the DELIVERED->RETURNED transition."""
    out = []
    ridx = 0
    for o in _all_orders():
        if o["_fate"] != "NORMAL" or o["_delivered_ts"] is None:
            continue                       # only delivered orders can be returned
        for ln in o["_lines"]:
            dec = _return_decision(ln["order_line_id"], o["_delivered_ts"], ln["quantity"], o["_incident"])
            if dec is None:
                continue
            ridx += 1
            out.append({"return_id": f"RET-{ridx:06d}", "order_line_id": ln["order_line_id"],
                        "return_ts": dec["return_ts"], "quantity": dec["quantity"], "reason": dec["reason"],
                        "_order_id": o["order_id"], "_product_id": ln["product_id"]})
    return out


# ======================================================================
# inventory: derived from the real sales + seeded restocks, running on_hand, reconciles, stockout at incident
# ======================================================================
@lru_cache(maxsize=None)
def _all_inventory():
    """Stock movements per (depot, product), built from the actual fulfilled sales plus restocks so on_hand is
    a true running balance. on_hand stays >= 0 everywhere except the deliberate Ares incident-product stockout."""
    # gather sales per (depot, product) from the real (clean) lines
    sales = defaultdict(list)
    for o in _all_orders():
        if o["_fate"] == "CANCELLED":
            continue                       # cancelled orders are not fulfilled, so no stock movement
        depot = o["warehouse_id"]
        for ln in o["_lines"]:
            sales[(depot, ln["product_id"])].append((ln["line_ts"], ln["quantity"]))
    inc_pid = _incident_product()
    out = []
    for (depot, pid), evs in sales.items():
        evs.sort(key=lambda x: x[0])
        is_incident_pair = (depot == INCIDENT_DEPOT and pid == inc_pid)
        by_day = defaultdict(list)
        for ts, q in evs:
            by_day[ts.date()].append((ts, q))
        daily_totals = {d: sum(q for _, q in by_day[d]) for d in by_day}
        # size stock to NORMAL daily demand. For the incident pair that excludes the spike day, so when the
        # restock is withheld on the incident day the buying spike drains it to zero (the stockout).
        if is_incident_pair:
            normal = [daily_totals[d] for d in by_day if _dayidx(d) not in INCIDENT_DAYS]
            sizing = max(normal) if normal else 50
        else:
            sizing = max(daily_totals.values())
        buffer = max(10, int(sizing * 0.5))
        on_hand = max(50, int(sizing * 1.5))       # initial stock covers a normal busy day
        movements = []
        for day in sorted(by_day):
            day_total = daily_totals[day]
            stockout_day = is_incident_pair and _dayidx(day) in INCIDENT_DAYS
            # (s, S) morning restock: top up to cover the day so on_hand never goes negative (except the stockout)
            if not stockout_day and on_hand < day_total:
                add = day_total + buffer - on_hand
                on_hand += add
                movements.append((dt.datetime.combine(day, dt.time()), add, on_hand))
            for ts, q in by_day[day]:
                if stockout_day and on_hand - q < 0:
                    q = on_hand                # stockout: fulfil only what is on hand, floor at 0 (backorder rest)
                if q == 0:
                    continue                   # nothing shipped (backordered), so no stock movement
                on_hand -= q
                movements.append((ts, -q, on_hand))
        # strictly increasing movement_ts per pair, so on_hand is a verifiable running balance under a ts sort
        prev = None
        for ts, delta, oh in movements:
            if prev is not None and ts <= prev:
                ts = prev + dt.timedelta(microseconds=1)
            prev = ts
            out.append({"warehouse_id": depot, "product_id": pid, "movement_ts": ts,
                        "delta": delta, "on_hand": oh})
    return out

# COMMAND ----------

# MAGIC %md
# MAGIC ## Per feed batch generators
# MAGIC
# MAGIC Each returns a plain list of dicts for the requested batch by slicing the universe to the batch\'s day. Writing
# MAGIC is a separate step, so the shape of the data is easy to read and test on its own.

# COMMAND ----------

# ======================================================================
# Per-batch slicers (what actually lands per batch)
# ======================================================================
def gen_warehouses(batch):
    return [dict(w) for w in warehouses_base()]


def gen_categories(batch):
    return [dict(c) for c in categories_base()]


def gen_suppliers(batch):
    return [dict(s) for s in suppliers_base()]


def gen_products(batch):
    return [{k: p[k] for k in ("product_id", "sku", "product_name", "category_id", "supplier_id",
                               "mass_kg", "hazard_class", "active")} for p in products_base()]


def gen_customers(batch):
    """Daily full snapshot, tier as of this batch (the tier SCD2 source)."""
    snap = _day(batch)
    out = []
    for c in customers_base():
        out.append({"customer_id": c["customer_id"], "customer_name": c["customer_name"],
                    "customer_type": c["customer_type"], "tier": _tier_at(c["_i"], batch),
                    "home_warehouse_id": c["home_warehouse_id"], "signup_date": c["signup_date"],
                    "snapshot_date": snap})
    return out


def gen_order_lines(batch):
    """Append fact: the lines whose line_ts falls on this batch's day, plus the source messiness (duplicate
    submissions, a few dirty rows for quarantine, and late arrivals from the far depots)."""
    date = _day(batch)
    r = _rng(_CH_MESS, batch)
    out = []
    for o in _all_orders():
        for ln in o["_lines"]:
            if ln["line_ts"].date() != date:
                continue
            out.append({"order_line_id": ln["order_line_id"], "order_id": o["order_id"],
                        "product_id": ln["product_id"], "customer_id": o["customer_id"],
                        "warehouse_id": o["warehouse_id"], "quantity": ln["quantity"], "line_ts": ln["line_ts"]})
    clean = list(out)
    # 2% duplicate submissions (same order_line_id re-emitted)
    for dup in r.sample(clean, k=min(len(clean), round(DUP_FRAC * len(clean)))):
        out.append(dict(dup))
    # 1.5% late arrivals dated to the previous day, concentrated on the far depots (batch >= 2). The event
    # time scatters within the previous day (never before the base date), so the line is still priceable.
    if batch >= 2:
        prevday = _day(batch) - dt.timedelta(days=1)
        far = [row for row in clean if row["warehouse_id"] in ("DEP-05", "DEP-06")]
        pool = far if far else clean
        for late in r.sample(pool, k=min(len(pool), round(LATE_FRAC * len(clean)))):
            row = dict(late)
            row["order_line_id"] = row["order_line_id"] + "-LATE"
            row["line_ts"] = dt.datetime.combine(prevday, dt.time()) + dt.timedelta(seconds=r.randint(0, 80000))
            out.append(row)
    # 1% dirty rows for quarantine: negative qty, null product, malformed ts
    n_dirty = round(DIRTY_FRAC * len(clean))
    for k in range(n_dirty):
        base = dict(r.choice(clean))
        base["order_line_id"] = base["order_line_id"] + f"-DIRTY{k}"
        mode = k % 3
        if mode == 0:
            base["quantity"] = -abs(r.randint(1, 5))    # negative quantity
        elif mode == 1:
            base["product_id"] = None                   # missing product (unknown SKU)
        else:
            base["quantity"] = 0                        # zero quantity (violates quantity > 0)
        out.append(base)
    r.shuffle(out)
    return out


def gen_orders(batch):
    """CDC change records whose change happens on this batch's day. INSERT at PLACED, UPDATE on each later
    transition, DELETE when CANCELLED-and-purged. change_seq strictly increasing per order_id, max == current."""
    date = _day(batch)
    out = []
    seqs = Counter()
    # Build cumulative change_seq across batches: replay all changes up to this batch in time order per order.
    for o in _all_orders():
        # changes up to and including this batch day
        prior_changes = [(st, ts) for st, ts in o["_timeline"] if ts.date() < date]
        today_changes = [(st, ts) for st, ts in o["_timeline"] if ts.date() == date]
        base_seq = len(prior_changes)
        for j, (st, ts) in enumerate(today_changes):
            seq = base_seq + j + 1
            if seq == 1:
                op = "INSERT"
            elif st == "CANCELLED" and (o["_cust_i"] % 4 == 0):
                op = "DELETE"        # a subset of cancellations are purged (soft delete)
            else:
                op = "UPDATE"
            out.append({"order_id": o["order_id"], "customer_id": o["customer_id"],
                        "warehouse_id": o["warehouse_id"], "channel": o["channel"],
                        "order_ts": o["order_ts"], "status": st, "op": op,
                        "change_ts": ts, "change_seq": seq})
    # one DELIVERED->RETURNED transition per order, at the earliest return that lands on this day
    ret_by_order = defaultdict(list)
    for ret in _all_returns():
        ret_by_order[ret["_order_id"]].append(ret["return_ts"])
    for oid, tss in ret_by_order.items():
        first = min(tss)
        if first.date() != date:
            continue
        o = _order_by_id()[oid]
        if o["_delivered_ts"] is None:
            continue
        out.append({"order_id": o["order_id"], "customer_id": o["customer_id"],
                    "warehouse_id": o["warehouse_id"], "channel": o["channel"],
                    "order_ts": o["order_ts"], "status": "RETURNED", "op": "UPDATE",
                    "change_ts": first, "change_seq": len(o["_timeline"]) + 1})
    _rng(_CH_ORDERS, batch + 555).shuffle(out)
    return out


@lru_cache(maxsize=None)
def _order_by_id():
    return {o["order_id"]: o for o in _all_orders()}


def gen_returns(batch):
    """Returns whose return_ts lands on this batch's day, plus a small fraction of orphan returns (bad FK)."""
    date = _day(batch)
    r = _rng(_CH_RETURNS, batch)
    out = []
    for ret in _all_returns():
        if ret["return_ts"].date() != date:
            continue
        out.append({"return_id": ret["return_id"], "order_line_id": ret["order_line_id"],
                    "return_ts": ret["return_ts"], "quantity": ret["quantity"], "reason": ret["reason"]})
    # orphan returns: reference a non-existent order_line_id (deliberate quarantine case)
    n_orphan = round(ORPHAN_RETURN_FRAC * len(out))
    for k in range(n_orphan):
        out.append({"return_id": f"RET-ORPHAN-{batch}-{k}", "order_line_id": f"OL-0-999999{k}",
                    "return_ts": dt.datetime.combine(date, dt.time()) + dt.timedelta(seconds=r.randint(0, 80000)),
                    "quantity": r.randint(1, 3), "reason": r.choice(RETURN_REASONS)})
    r.shuffle(out)
    return out


def gen_inventory(batch):
    """Stock movements whose movement_ts lands on this batch's day."""
    date = _day(batch)
    out = []
    for m in _all_inventory():
        if m["movement_ts"].date() != date:
            continue
        out.append({"warehouse_id": m["warehouse_id"], "product_id": m["product_id"],
                    "movement_ts": m["movement_ts"], "delta": m["delta"], "on_hand": m["on_hand"]})
    out.sort(key=lambda m: (m["warehouse_id"], m["product_id"], m["movement_ts"]))
    return out

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write helpers
# MAGIC
# MAGIC `coalesce(1)` keeps each drop to a single small file, realistic for these volumes and tidy in the landing
# MAGIC zone. `overwrite` on the batch folder makes the generator rerunnable: landing the same batch twice leaves the
# MAGIC same files, never doubles them.

# COMMAND ----------

def landing_root(catalog: str) -> str:
    """Root of the landing Volume for a student catalog."""
    return f"/Volumes/{catalog}/helios_raw/helios_landing"


def _to_rows(dicts: list, schema: T.StructType) -> list:
    """Map dicts to positional tuples in schema field order (avoids the Row(**kwargs) key-sorting trap)."""
    names = [f.name for f in schema.fields]
    return [tuple(d.get(n) for n in names) for d in dicts]


def _write(dicts, schema, fmt, path, **options):
    df = spark.createDataFrame(_to_rows(dicts, schema), schema).coalesce(1)
    writer = df.write.mode("overwrite")
    for k, val in options.items():
        writer = writer.option(k, val)
    writer.format(fmt).save(path)
    return len(dicts)

# COMMAND ----------

# MAGIC %md
# MAGIC ## `generate_batch`: the one entry point a lab calls

# COMMAND ----------

def generate_batch(catalog: str, batch: int = 1, land_reference: bool | None = None) -> dict:
    """Land one batch of all source feeds into the student\'s landing Volume.

    catalog          the student catalog, e.g. labs_jane_doe (from the identity cell).
    batch            which batch to land. Batch 1 is the clean initial load. Batch >= 2 adds CDC order
                     transitions, SCD2 price changes, tier promotions, returns, late and duplicate records,
                     and late and duplicate records.
    land_reference   whether to land the stable reference feeds (products, categories, suppliers, warehouses).
                     Defaults to True on batch 1 only.

    Returns a dict of row counts written per feed. Idempotent: rerunning a batch overwrites its folders.
    Folders use plain names (no key=value), so Spark does not infer partition columns on read.
    """
    root = landing_root(catalog)
    day = _day(batch)
    if land_reference is None:
        land_reference = (batch == 1)
    counts = {}

    counts["order_lines"] = _write(gen_order_lines(batch), ORDER_LINES_SCHEMA, "json",
                                   f"{root}/order_lines/batch_{batch}")
    counts["orders"] = _write(gen_orders(batch), ORDERS_SCHEMA, "json", f"{root}/orders/batch_{batch}")
    counts["inventory"] = _write(gen_inventory(batch), INVENTORY_SCHEMA, "json",
                                 f"{root}/inventory/batch_{batch}")
    counts["price_list"] = _write(gen_price_list(batch), PRICE_LIST_SCHEMA, "csv",
                                  f"{root}/price_list/batch_{batch}", header="true")
    counts["customers"] = _write(gen_customers(batch), CUSTOMERS_SCHEMA, "parquet",
                                 f"{root}/customers/snapshot_{day.isoformat()}")

    returns_rows = gen_returns(batch)
    if returns_rows:                          # returns arrive from batch 2 (nothing has been delivered yet on day 1)
        counts["returns"] = _write(returns_rows, RETURNS_SCHEMA, "json", f"{root}/returns/batch_{batch}")

    if land_reference:
        counts["products"] = _write(gen_products(batch), PRODUCTS_SCHEMA, "parquet",
                                    f"{root}/products/batch_{batch}")
        counts["categories"] = _write(gen_categories(batch), CATEGORIES_SCHEMA, "parquet",
                                      f"{root}/categories/batch_{batch}")
        counts["suppliers"] = _write(gen_suppliers(batch), SUPPLIERS_SCHEMA, "json",
                                     f"{root}/suppliers/batch_{batch}")
        counts["warehouses"] = _write(gen_warehouses(batch), WAREHOUSES_SCHEMA, "json",
                                      f"{root}/warehouses/batch_{batch}")

    print(f"Landed batch {batch} for {catalog} (day {day.isoformat()}):")
    for feed, c in counts.items():
        print(f"  {feed:<14} {c:>7} rows  ->  {root}/{feed}/")
    return counts


def clear_landing(catalog: str) -> None:
    """Remove every landed file for a fresh start. Used by full resets, not by routine reruns."""
    root = landing_root(catalog)
    try:
        dbutils.fs.rm(root, recurse=True)
        print(f"Cleared landing zone {root}")
    except Exception as e:  # noqa: BLE001
        print(f"Nothing to clear at {root} ({e})")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Usage
# MAGIC ```python
# MAGIC %run ../00_setup/data_generator
# MAGIC generate_batch(catalog, batch=1)
# MAGIC ```
# MAGIC Nothing runs on `%run` alone, so importing the functions is always safe.