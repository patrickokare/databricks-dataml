# Helios Trading Corporation: The Ten Source Feeds

Every feed arrives as files in a Unity Catalog Volume. Another system writes the files and we pick them up.

| Feed | Format | Frequency | What we get each time |
|---|---|---|---|
| order_lines | JSON | daily, high volume | new sales lines only |
| orders | JSON | daily, high volume | one row per change to an order |
| returns | JSON | daily, growing | new returns, days after the sale they relate to |
| inventory | JSON | daily, high volume | new stock movements only |
| price_list | CSV with header | daily | every price, with start and end dates |
| customers | Parquet | daily | the whole customer list again |
| products | Parquet | only when updated | the full product list |
| categories | Parquet | only when updated | the full category list |
| suppliers | JSON | only when updated | the full supplier list |
| warehouses | JSON | only when updated | the full depot list |

Three formats (JSON, CSV and Parquet), and four different ways of arriving: new rows only, a row per change, a full copy every day,
and a full snapshot.

The products, categories, suppliers and warehouses feed do not arrive on a schedule. They turn up when someone changes the catalogue, adds a
supplier or opens a depot, which might be today and might be next quarter. Most runs will find no file for
them, and the odd run will find one. 

One note: JSON columns come back in alphabetical order, because that is what Spark does when it
infers a schema from JSON. Parquet and CSV keep the order they were written in. The samples below show each
feed as it reads back.

---

## 1. order_lines

The sales feed. 

One row per product line on an order: who bought what, how many, from which depot, and when.

| customer_id | line_ts | order_id | order_line_id | product_id | quantity | warehouse_id |
|---|---|---|---|---|---|---|
| CUST-0284 | 2257-03-01T10:39:10.000Z | ORD-010338 | OL-1-031366 | PRD-036 | 5 | DEP-03 |
| CUST-0214 | 2257-03-01T13:36:08.000Z | ORD-015000 | OL-1-045366 | PRD-132 | 22 | DEP-05 |
| CUST-0019 | 2257-03-01T14:31:13.000Z | ORD-005716 | OL-1-017272 | PRD-053 | 1 | DEP-02 |
| CUST-0038 | 2257-03-01T18:18:22.000Z | ORD-010928 | OL-1-033149 | PRD-080 | 2 | DEP-03 |
| CUST-0080 | 2257-03-01T05:51:17.000Z | ORD-011357 | OL-1-034441 | PRD-057 | 5 | DEP-03 |

| Column | Type | What it means |
|---|---|---|
| order_line_id | STRING | the line's own id, and the only thing we can use to spot duplicates |
| line_ts | TIMESTAMP | when the line was placed |
| order_id | STRING | the order this line belongs to, links to `orders` |
| product_id | STRING | what was bought, links to `products` |
| customer_id | STRING | who bought it |
| warehouse_id | STRING | the depot fulfilling the line |
| quantity | INT | units ordered |

**Behaviour**

- The same line can be sent to us twice, so you will find repeated `order_line_id` values.
- Some rows turn up a day late, dated to the day before. Mostly from the far depots, where the connection
  is poor.
- A few rows are bad: negative quantity, zero quantity, or a missing `product_id`.
- Rows are only ever added. Nothing here is updated or deleted later.
- There is no price on this feed, so it tells us what sold but not what it was worth.

**Design considerations**

- Removing the duplicates, using `order_line_id`.
- What we do with the bad rows, and how anyone finds out we dropped them.
- Late rows are dated yesterday but arrive today. Which date do we count them under?
- Whether we reload everything each run or only the new rows.

---

## 2. orders

Every change to an order, sent as CDC (change data capture). One row per change, so a single order shows up
here several times as it moves from placed to delivered. The sample below is one order doing exactly that,
five changes in one day.

| change_seq | change_ts | channel | customer_id | op | order_id | order_ts | status | warehouse_id |
|---|---|---|---|---|---|---|---|---|
| 1 | 2257-03-01T00:42:34.000Z | CONSOLE | CUST-0072 | INSERT | ORD-000006 | 2257-03-01T00:42:34.000Z | PLACED | DEP-01 |
| 2 | 2257-03-01T04:42:34.000Z | CONSOLE | CUST-0072 | UPDATE | ORD-000006 | 2257-03-01T00:42:34.000Z | PAID | DEP-01 |
| 3 | 2257-03-01T13:42:34.000Z | CONSOLE | CUST-0072 | UPDATE | ORD-000006 | 2257-03-01T00:42:34.000Z | PICKED | DEP-01 |
| 4 | 2257-03-01T19:42:34.000Z | CONSOLE | CUST-0072 | UPDATE | ORD-000006 | 2257-03-01T00:42:34.000Z | SHIPPED | DEP-01 |
| 5 | 2257-03-01T23:42:34.000Z | CONSOLE | CUST-0072 | UPDATE | ORD-000006 | 2257-03-01T00:42:34.000Z | DELIVERED | DEP-01 |

| Column | Type | What it means |
|---|---|---|
| order_id | STRING | the order this change applies to |
| change_seq | BIGINT | counts up within an `order_id`, so the highest number is the latest state |
| change_ts | TIMESTAMP | when the change happened, and it can arrive out of order |
| op | STRING | what kind of change: INSERT, UPDATE, DELETE |
| status | STRING | PLACED, PAID, PICKED, SHIPPED, DELIVERED, BACKORDERED, CANCELLED, RETURNED |
| channel | STRING | how the order was placed: CONSOLE, API, COUNTER |
| order_ts | TIMESTAMP | when the order was first placed, the same on every change |
| customer_id | STRING | who placed the order |
| warehouse_id | STRING | the depot fulfilling it |

**Behaviour**

- Orders move forward through PLACED, PAID, PICKED, SHIPPED, DELIVERED and never go backwards.
- BACKORDERED slots in after payment when stock runs out. CANCELLED and RETURNED are the end of the line.
- Lots of orders are part way through at any given time, so plenty never reach DELIVERED in our data.
- Some cancellations come through as `op = DELETE` instead of a status change.
- `change_ts` can be out of order and cannot be relied on. `change_seq` can.

**Design considerations**

- Working out the current state of an order when we get several changes for it at once.
- Whether we use `change_ts` for anything at all.
- What a DELETE means for us: do we delete the order or keep it and mark it.
- This feed and `order_lines` do not match day for day, because an order can change on a day when none of
  its lines arrive.

---

## 3. returns

Goods sent back, linked to the original order line.

| order_line_id | quantity | reason | return_id | return_ts |
|---|---|---|---|---|
| OL-1-016977 | 5 | FAULTY | RET-000973 | 2257-03-02T22:42:24.000Z |
| OL-1-018165 | 9 | NOT_NEEDED | RET-001039 | 2257-03-02T23:55:09.000Z |
| OL-1-036802 | 9 | FAULTY | RET-002085 | 2257-03-02T22:54:43.000Z |
| OL-1-040070 | 5 | FAULTY | RET-002242 | 2257-03-02T21:50:03.000Z |

| Column | Type | What it means |
|---|---|---|
| return_id | STRING | the return's own id |
| order_line_id | STRING | the line being returned, links back to `order_lines` |
| return_ts | TIMESTAMP | when it was returned, always after the original sale |
| quantity | INT | units returned, never more than were bought |
| reason | STRING | FAULTY, WRONG_PART, NOT_NEEDED, DAMAGED_IN_TRANSIT |

**Behaviour**

- A return points at a line that sold days earlier, so the sale and the return reach us well apart.
- Some returns point at an `order_line_id` that does not exist.
- A return also flips its order to RETURNED on the `orders` feed, so we see the same event twice.
- There is no money on this feed, only quantities.

**Design considerations**

- A refund is worth what the customer paid on the day, not what the product costs now.
- What we do with returns that point at nothing: reject, park them somewhere, or let them through.
- Return rate is not today's returns over today's sales, because the two are days apart.

---

## 4. inventory

Stock going in and out of each depot, with the balance after every movement.

| delta | movement_ts | on_hand | product_id | warehouse_id |
|---|---|---|---|---|
| -4 | 2257-03-01T00:28:22.000Z | 311 | PRD-001 | DEP-01 |
| -4 | 2257-03-01T00:41:03.000Z | 307 | PRD-001 | DEP-01 |
| -5 | 2257-03-01T01:11:22.000Z | 302 | PRD-001 | DEP-01 |
| -2 | 2257-03-01T01:15:11.000Z | 300 | PRD-001 | DEP-01 |
| -4 | 2257-03-01T02:38:01.000Z | 296 | PRD-001 | DEP-01 |

| Column | Type | What it means |
|---|---|---|
| warehouse_id | STRING | which depot |
| product_id | STRING | which product |
| movement_ts | TIMESTAMP | when it moved, always increasing for a given depot and product |
| delta | INT | the change, negative on a sale, positive on a restock |
| on_hand | INT | how much is left after this movement |

**Behaviour**

- `on_hand` is the previous `on_hand` plus `delta`. Read it down the sample: 311, 307, 302, 300, 296.
- Movements come from real sales, so this feed and `order_lines` should agree with each other.
- `on_hand` never goes below zero. It does hit zero when a depot genuinely runs out, and the extra demand
  gets backordered.
- When stock runs out the sale does not happen, so a few order lines have no movement against them.

**Design considerations**

- Whether we keep every movement, just the latest balance per depot and product, or both.
- The balance only makes sense in `movement_ts` order.
- We could use this feed to check our sales numbers, if we decide it is worth the effort.

---

## 5. price_list

What each product sells for and what it costs us. Prices are dated, so the file holds the old prices as well
as the current one.

| product_id | supplier_id | unit_price | unit_cost | currency | effective_from | effective_to | is_current |
|---|---|---|---|---|---|---|---|
| PRD-001 | SUP-02 | 269.99 | 183.59 | CREDITS | 2257-03-01 | | true |
| PRD-002 | SUP-03 | 213.99 | 149.79 | CREDITS | 2257-03-01 | | true |
| PRD-003 | SUP-04 | 3444.99 | 1929.19 | CREDITS | 2257-03-01 | | true |
| PRD-004 | SUP-05 | 833.99 | 533.75 | CREDITS | 2257-03-01 | | true |
| PRD-005 | SUP-06 | 353.99 | 247.79 | CREDITS | 2257-03-01 | | true |

| Column | Type | What it means |
|---|---|---|
| product_id | STRING | the product this price is for |
| supplier_id | STRING | who supplies it at this price |
| unit_price | DECIMAL(12,2) | what the customer pays |
| unit_cost | DECIMAL(12,2) | what we pay the supplier |
| currency | STRING | always CREDITS |
| effective_from | DATE | the day this price started |
| effective_to | DATE | the day it stopped, empty if it is the current one |
| is_current | BOOLEAN | true for the price in use now |

**Behaviour**

- The only feed with prices on it, so no revenue number exists without it.
- Every delivery is the whole list: current prices plus all the old ones.
- The file gets bigger as prices change, but the number of current prices stays the same.
- Prices do change during the period, including one that drops below what we pay for the product.
- It is CSV, so every column arrives as text unless we set the types ourselves.

**Design considerations**

- To value a sale we have to find the price that was in use on the day of that sale.
- We have to keep the old prices, not overwrite them, which makes this a slowly changing dimension.
- We choose the types here. Using a float for money is how rounding errors end up in the accounts.
- A price below cost looks like a perfectly valid row and is still very wrong.

---

## 6. customers

Every customer, sent in full once a day. The whole list each time, whether anything changed or not.

| customer_id | customer_name | customer_type | tier | home_warehouse_id | signup_date | snapshot_date |
|---|---|---|---|---|---|---|
| CUST-0001 | Anya Idris | COLONIAL | DRIFTER | DEP-02 | 2254-10-30 | 2257-03-01 |
| CUST-0002 | Ben Sato | FREIGHT_FLEET | TRADE | DEP-03 | 2255-09-11 | 2257-03-01 |
| CUST-0003 | Cass Diaz | MINER | DRIFTER | DEP-04 | 2256-05-25 | 2257-03-01 |
| CUST-0004 | Dmitri Lindgren | RESEARCH | TRADE | DEP-05 | 2256-01-21 | 2257-03-01 |
| CUST-0005 | Elena Tanaka | COLONIAL | CHARTER | DEP-06 | 2255-11-21 | 2257-03-01 |

| Column | Type | What it means |
|---|---|---|
| customer_id | STRING | the customer id, appears once in each day's file |
| customer_name | STRING | the fleet or company name |
| customer_type | STRING | what kind of customer: FREIGHT_FLEET, MINER, LINER, RESEARCH, PATROL, INDEPENDENT, COLONIAL |
| tier | STRING | CHARTER, TRADE or DRIFTER, which affects the price they pay |
| home_warehouse_id | STRING | the depot they are registered at |
| signup_date | DATE | when they started buying from us |
| snapshot_date | DATE | the day this copy of the list was taken |

**Behaviour**

- What a customer buys follows their `customer_type`. A miner buys propulsion and power, a liner buys life
  support and consumables.
- Customers of the same type tend to use the same depot, so each depot sells a different mix.
- Customers move up a tier over time, and nothing in the file tells us that happened.
- The only way to spot a change is to compare one day's file with the next.
- It is Parquet, so the types come already set.

**Design considerations**

- Spotting a tier change by comparing one day against the previous one.
- Keeping the old tier, so a sale made when someone was a DRIFTER is not reported as a TRADE sale.
- The same 300 rows arrive every day, most of them unchanged, so we need a plan for what to keep.

---

## 7. products

The parts catalogue. One row per item we sell.

| product_id | sku | product_name | category_id | supplier_id | mass_kg | hazard_class | active |
|---|---|---|---|---|---|---|---|
| PRD-001 | HUL-1001 | Hull And Structure unit 001 | CAT-2 | SUP-02 | 1314.17 | CRYOGENIC | true |
| PRD-002 | LIF-1002 | Life Support unit 002 | CAT-3 | SUP-03 | 444.07 | CRYOGENIC | true |
| PRD-003 | POW-1003 | Power unit 003 | CAT-4 | SUP-04 | 117.83 | CRYOGENIC | true |
| PRD-004 | NAV-1004 | Navigation unit 004 | CAT-5 | SUP-05 | 859.93 | null | true |
| PRD-005 | THE-1005 | Thermal unit 005 | CAT-6 | SUP-06 | 86.86 | CRYOGENIC | true |

| Column | Type | What it means |
|---|---|---|
| product_id | STRING | the product id |
| sku | STRING | the code people use for it |
| product_name | STRING | the description |
| category_id | STRING | links to `categories` |
| supplier_id | STRING | who supplies it |
| mass_kg | DOUBLE | shipping weight |
| hazard_class | STRING | FLAMMABLE, CRYOGENIC, RADIOACTIVE, or null if it is nothing special |
| active | BOOLEAN | whether we still sell it |

**Behaviour**

- Only arrives when the catalogue changes, so most runs will find nothing new.
- `hazard_class` is null for most products, and that is correct, not missing data.
- Some products are switched off but still show up in older sales.
- No price here. Prices change often and the catalogue does not, so they are kept apart.

**Design considerations**

- A small list that changes now and then is the one case where just overwriting it is the right answer.
- The pipeline still has to handle the usual case of no new file at all.
- A blanket rule that rejects nulls would throw out most of the catalogue.
- A switched off product still has to join, or older sales lose their product details.

---

## 8. categories

The grouping above products. Seven categories rolling up into five departments. This is the whole feed:

| category_id | category_name | department |
|---|---|---|
| CAT-1 | PROPULSION | PROPULSION_AND_POWER |
| CAT-2 | HULL_AND_STRUCTURE | STRUCTURE |
| CAT-3 | LIFE_SUPPORT | HABITAT |
| CAT-4 | POWER | PROPULSION_AND_POWER |
| CAT-5 | NAVIGATION | AVIONICS |
| CAT-6 | THERMAL | HABITAT |
| CAT-7 | CONSUMABLES | CONSUMABLES |

| Column | Type | What it means |
|---|---|---|
| category_id | STRING | the category id, links from `products` |
| category_name | STRING | the category |
| department | STRING | the level above, shared by more than one category |

**Behaviour**

- Only changes if the business reorganises, so it changes less than anything else here.
- Seven rows, the smallest feed by a long way.
- Almost every report groups by category or department, so it gets used constantly.

**Design considerations**

- The categories are wildly different commercially. Consumables are most of the units sold and almost none
  of the money. Propulsion and power are the other way round.
- A report counting units and a report summing revenue will tell completely different stories.
- Two levels to roll up through, so the join from a sale up to a department needs to be simple and reliable.

---

## 9. suppliers

Who we buy from.

| active | home_region | supplier_id | supplier_name |
|---|---|---|---|
| true | OUTER | SUP-01 | Lupus Supply 01 |
| true | OUTER | SUP-02 | Lyra Supply 02 |
| true | OUTER | SUP-03 | Hydra Supply 03 |
| true | INNER | SUP-04 | Lyra Supply 04 |
| true | INNER | SUP-05 | Phoenix Supply 05 |

| Column | Type | What it means |
|---|---|---|
| supplier_id | STRING | the supplier id |
| supplier_name | STRING | their name |
| home_region | STRING | INNER, BELT or OUTER |
| active | BOOLEAN | whether we still buy from them |

**Behaviour**

- Arrives when the supplier list changes, so when a contract is signed or a supplier stops trading.
- The only place a `supplier_id` gets turned into a name.
- `supplier_id` is also on `products` and on `price_list`, so there are two ways to get from a sale to a
  supplier.

**Design considerations**

- Two routes to the same supplier means the numbers can disagree, so we need to pick one and stick to it.
- Supplier is how the business will want to dig into a pricing problem, so the join has to be right even
  though the feed is tiny.

---

## 10. warehouses

The six depots. This is the whole feed:

| body | region | uplink_reliability | warehouse_id | warehouse_name |
|---|---|---|---|---|
| Luna | INNER | 0.99 | DEP-01 | Helios Prime |
| Luna | INNER | 0.98 | DEP-02 | Luna Hub |
| Mars | INNER | 0.95 | DEP-03 | Ares Depot |
| Belt | BELT | 0.9 | DEP-04 | Ceres Exchange |
| Europa | OUTER | 0.8 | DEP-05 | Europa Outpost |
| Titan | OUTER | 0.85 | DEP-06 | Titan Yard |

| Column | Type | What it means |
|---|---|---|
| warehouse_id | STRING | the depot id, used on five other feeds |
| warehouse_name | STRING | the depot name |
| body | STRING | the planet or moon it is on |
| region | STRING | INNER, BELT or OUTER |
| uplink_reliability | DOUBLE | how good the depot's data connection is, 0 to 1 |

**Behaviour**

- Only changes when a depot opens, closes or is reorganised, so almost never.
- The depots sell different things, because of who is registered at each one. Luna Hub does high volume
  passenger trade, Ares Depot does heavy industry and freight, Ceres Exchange serves mining rigs.
- `uplink_reliability` is lowest at Europa Outpost and Titan Yard, and those are the depots the late and
  duplicated rows in `order_lines` come from.

**Design considerations**

- Depot is the first thing anyone asks to see the numbers by, so it has to be on every fact table.
- Data quality varies by depot, so our quality reporting has to break down by depot too. An overall 99%
  pass rate can hide one depot sitting at 80%.

---

## What the ten feeds tell us

**Three formats, and only one of them sets its own types.** Parquet comes typed, JSON gets inferred, CSV is
text until we say otherwise. The prices and costs arrive in the CSV.

**Four ways of arriving, and each one needs handling differently.** Rows that only get added, a row per
change, a full copy every day, and lists that turn up whenever someone edits them. Treating all four the
same way is the usual mistake.

**None of it is clean.** Duplicates, late rows, bad quantities, missing products, returns pointing at
nothing, and a price below cost.

**No feed answers a business question on its own.** `order_lines` has no money on it, `price_list` has no
sales, `returns` has neither. Revenue only exists once we join a sale to the price that applied that day.

**The source barely keeps any history.** Prices keep their old versions, customers do not, and the daily
customer file never says what changed. Keep only the latest and we cannot answer what something was worth
at the time.

**The volumes are real and they grow.** Rebuilding everything from scratch on every run is a decision worth
making deliberately now, rather than finding out later.

Those are the constraints. The data contract is where we decide what to do about them.
