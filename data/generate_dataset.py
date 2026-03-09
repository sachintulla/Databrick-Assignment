"""
Synthetic SuperStore Sales Dataset Generator.

Generates a realistic ~9,000-row SuperStore dataset mirroring the
original Tableau SuperStore structure — used when the real dataset
is not available locally.

Run:
    python data/generate_dataset.py
"""

import os
import random
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

random.seed(42)
np.random.seed(42)

# ── Reference data ──────────────────────────────────────────────────────────

REGIONS = ["East", "West", "Central", "South"]

STATE_REGION = {
    "New York": "East",    "Pennsylvania": "East",  "Massachusetts": "East",
    "New Jersey": "East",  "Connecticut": "East",   "Maine": "East",
    "California": "West",  "Washington": "West",    "Oregon": "West",
    "Nevada": "West",      "Arizona": "West",       "Colorado": "West",
    "Texas": "Central",    "Illinois": "Central",   "Ohio": "Central",
    "Michigan": "Central", "Indiana": "Central",    "Wisconsin": "Central",
    "Florida": "South",    "Georgia": "South",      "North Carolina": "South",
    "Virginia": "South",   "Tennessee": "South",    "Alabama": "South",
}

CITIES_BY_STATE = {
    "New York": ["New York City", "Buffalo", "Albany"],
    "California": ["Los Angeles", "San Francisco", "San Diego", "Sacramento"],
    "Texas": ["Houston", "Dallas", "Austin", "San Antonio"],
    "Florida": ["Miami", "Orlando", "Tampa", "Jacksonville"],
    "Illinois": ["Chicago", "Springfield", "Naperville"],
    "Pennsylvania": ["Philadelphia", "Pittsburgh", "Allentown"],
    "Ohio": ["Columbus", "Cleveland", "Cincinnati"],
    "Georgia": ["Atlanta", "Savannah", "Augusta"],
    "Washington": ["Seattle", "Spokane", "Tacoma"],
    "Michigan": ["Detroit", "Grand Rapids", "Lansing"],
    "North Carolina": ["Charlotte", "Raleigh", "Durham"],
    "Virginia": ["Virginia Beach", "Richmond", "Norfolk"],
    "Massachusetts": ["Boston", "Worcester", "Springfield"],
    "Tennessee": ["Nashville", "Memphis", "Knoxville"],
    "Indiana": ["Indianapolis", "Fort Wayne", "Evansville"],
    "Wisconsin": ["Milwaukee", "Madison", "Green Bay"],
    "Colorado": ["Denver", "Colorado Springs", "Aurora"],
    "Oregon": ["Portland", "Salem", "Eugene"],
    "Nevada": ["Las Vegas", "Reno", "Henderson"],
    "Arizona": ["Phoenix", "Tucson", "Scottsdale"],
    "Alabama": ["Birmingham", "Montgomery", "Huntsville"],
    "New Jersey": ["Newark", "Jersey City", "Paterson"],
    "Connecticut": ["Bridgeport", "New Haven", "Hartford"],
    "Maine": ["Portland", "Lewiston", "Bangor"],
}

SEGMENTS = ["Consumer", "Corporate", "Home Office"]
SHIP_MODES = ["Standard Class", "Second Class", "First Class", "Same Day"]

CATEGORIES = {
    "Furniture": {
        "Bookcases": [
            ("FUR-BO-10001798", "Bush Somerset Collection Bookcase", 260, 30),
            ("FUR-BO-10003472", "Sauder Classic Bookcase", 325, 40),
            ("FUR-BO-10004262", "Ikea Stockholm Bookshelf", 150, 20),
        ],
        "Chairs": [
            ("FUR-CH-10000454", "Hon Deluxe Fabric Upholstered Stacking Chair", 310, 50),
            ("FUR-CH-10001215", "Global Ergonomic Managers Chair", 812, 80),
            ("FUR-CH-10002774", "Safco PVC Mesh Chair", 495, 60),
        ],
        "Furnishings": [
            ("FUR-FU-10001487", "Eldon Fold N Roll Cart System", 22, 5),
            ("FUR-FU-10003710", "Tensor Magnetic Lamp", 38, 8),
            ("FUR-FU-10000944", "Brass Tacks Decorative Brass Bookends", 15, 4),
        ],
        "Tables": [
            ("FUR-TA-10000577", "Bretford CR4500 Series Slim Rectangular Table", 958, 120),
            ("FUR-TA-10001539", "Chromcraft Coffee Table", 340, 50),
            ("FUR-TA-10004091", "O Sullivan Console Storage Table", 1230, 150),
        ],
    },
    "Office Supplies": {
        "Appliances": [
            ("OFF-AP-10002311", "Hoover Upright Deep Cleaner", 159, 25),
            ("OFF-AP-10001492", "Belkin F5C206V Mini SurgeStrip", 45, 10),
            ("OFF-AP-10002892", "Cuisinart Brew Central 12 Cup Coffeemaker", 89, 15),
        ],
        "Art": [
            ("OFF-AR-10003056", "Avery 50 Box Pack Dividers", 8, 2),
            ("OFF-AR-10000330", "Newell 318", 12, 3),
            ("OFF-AR-10002399", "Sanford Pencils", 6, 2),
        ],
        "Binders": [
            ("OFF-BI-10003910", "Avery Non-Stick Binders", 3, 1),
            ("OFF-BI-10001634", "Fellowes Binder", 7, 2),
            ("OFF-BI-10004182", "GBC Premium Transparent Covers", 14, 3),
        ],
        "Envelopes": [
            ("OFF-EN-10001509", "Poly String Tie Envelopes", 7, 2),
            ("OFF-EN-10002886", "Kraft Clasp Envelopes", 5, 1),
        ],
        "Fasteners": [
            ("OFF-FA-10000304", "Advantage Rubber Bands", 2, 1),
            ("OFF-FA-10003030", "Universal Small Binder Clips", 3, 1),
        ],
        "Labels": [
            ("OFF-LA-10002762", "Avery Address Labels", 8, 2),
            ("OFF-LA-10003285", "Avery Self-Adhesive Name Badges", 5, 1),
        ],
        "Paper": [
            ("OFF-PA-10001970", "Xerox 1967", 7, 2),
            ("OFF-PA-10002365", "Hammermill Copy Plus Paper", 10, 3),
            ("OFF-PA-10003696", "Avery Printable Postcards", 8, 2),
        ],
        "Storage": [
            ("OFF-ST-10000107", "Fellowes Super Stor/Drawer", 45, 10),
            ("OFF-ST-10001888", "Advantus Plastic Paper Clips", 5, 1),
            ("OFF-ST-10004186", "Storex Durable Binder", 16, 4),
        ],
        "Supplies": [
            ("OFF-SU-10001148", "Acme Forged Steel Scissors", 14, 4),
            ("OFF-SU-10004002", "OIC Push Pins", 3, 1),
        ],
    },
    "Technology": {
        "Accessories": [
            ("TEC-AC-10003033", "Memorex Floppy Drive Ext USB", 25, 8),
            ("TEC-AC-10002167", "Belkin F5U069 USB 2.0 HUB", 35, 10),
            ("TEC-AC-10001498", "Verbatim 99753 External Hard Drive", 130, 25),
            ("TEC-AC-10004633", "Plantronics CS540 Wireless Headset", 325, 60),
        ],
        "Copiers": [
            ("TEC-CO-10004722", "Sharp BL-N1045 Copier", 5740, 800),
            ("TEC-CO-10000786", "Hewlett Packard LaserJet 1200", 950, 150),
        ],
        "Machines": [
            ("TEC-MA-10002412", "Brother Fax Machine", 290, 50),
            ("TEC-MA-10000822", "Fellowes PB500 Electric Punch Plastic", 1630, 200),
        ],
        "Phones": [
            ("TEC-PH-10002033", "Motorola Smart Phone", 350, 80),
            ("TEC-PH-10003988", "Samsung Smart Phone", 899, 150),
            ("TEC-PH-10001354", "Cisco SPA501G IP Phone", 213, 40),
            ("TEC-PH-10004664", "Apple iPhone 13", 1099, 200),
        ],
    },
}

FIRST_NAMES = [
    "James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael",
    "Linda", "William", "Barbara", "David", "Elizabeth", "Richard", "Susan",
    "Joseph", "Jessica", "Thomas", "Sarah", "Charles", "Karen",
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
    "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez",
    "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
]


# ── Helpers ──────────────────────────────────────────────────────────────────

def random_date(start: datetime, end: datetime) -> datetime:
    return start + timedelta(days=random.randint(0, (end - start).days))


def generate_customers(n: int = 793) -> list[dict]:
    customers = []
    for i in range(1, n + 1):
        name = f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"
        customers.append({
            "Customer ID": f"CUS-{i:05d}",
            "Customer Name": name,
            "Segment": random.choice(SEGMENTS),
        })
    return customers


def generate_rows(n_orders: int = 5009) -> list[dict]:
    customers = generate_customers()
    rows = []
    row_id = 1

    start_date = datetime(2021, 1, 1)
    end_date = datetime(2024, 12, 31)

    for order_num in range(1, n_orders + 1):
        order_id = f"CA-{random.randint(2021,2024)}-{random.randint(100000,999999)}"
        order_date = random_date(start_date, end_date)
        ship_days = random.randint(1, 7)
        ship_date = order_date + timedelta(days=ship_days)

        state = random.choice(list(STATE_REGION.keys()))
        region = STATE_REGION[state]
        city_list = CITIES_BY_STATE.get(state, [state + " City"])
        city = random.choice(city_list)
        postal_code = str(random.randint(10000, 99999))

        customer = random.choice(customers)
        ship_mode = random.choice(SHIP_MODES)

        # 1–3 line items per order
        n_items = random.randint(1, 3)
        for _ in range(n_items):
            category = random.choice(list(CATEGORIES.keys()))
            subcategory = random.choice(list(CATEGORIES[category].keys()))
            product_id, product_name, base_price, base_profit = random.choice(
                CATEGORIES[category][subcategory]
            )

            quantity = random.randint(1, 10)

            # Discount skewed towards 0 (most products have no / small discount)
            discount = random.choices(
                [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8],
                weights=[40, 20, 15, 8, 5, 4, 3, 3, 2],
            )[0]

            sales = round(base_price * quantity * (1 - discount * 0.5), 2)

            # High discount → potential loss
            profit_multiplier = 1 - (discount * 2.5)
            profit = round(base_profit * quantity * profit_multiplier, 2)

            # Add noise
            profit += round(random.uniform(-base_profit * 0.1, base_profit * 0.1), 2)

            rows.append({
                "Row ID": row_id,
                "Order ID": order_id,
                "Order Date": order_date.strftime("%Y-%m-%d"),
                "Ship Date": ship_date.strftime("%Y-%m-%d"),
                "Ship Mode": ship_mode,
                "Customer ID": customer["Customer ID"],
                "Customer Name": customer["Customer Name"],
                "Segment": customer["Segment"],
                "Country": "United States",
                "City": city,
                "State": state,
                "Postal Code": postal_code,
                "Region": region,
                "Product ID": product_id,
                "Category": category,
                "Sub-Category": subcategory,
                "Product Name": product_name,
                "Sales": sales,
                "Quantity": quantity,
                "Discount": discount,
                "Profit": profit,
            })
            row_id += 1

    return rows


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    output_dir = Path(__file__).parent / "raw"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "superstore_sales.csv"

    print("Generating SuperStore synthetic dataset ...")
    rows = generate_rows(n_orders=5009)
    df = pd.DataFrame(rows)

    # Intentionally inject ~1% nulls and ~0.5% duplicates for cleaning exercises
    null_idx = df.sample(frac=0.01, random_state=1).index
    df.loc[null_idx, "Postal Code"] = None

    dup_rows = df.sample(frac=0.005, random_state=2)
    df = pd.concat([df, dup_rows], ignore_index=True)

    df.to_csv(output_path, index=False)
    print(f"Dataset saved  → {output_path}")
    print(f"Total rows     : {len(df):,}")
    print(f"Unique orders  : {df['Order ID'].nunique():,}")
    print(f"Date range     : {df['Order Date'].min()} → {df['Order Date'].max()}")


if __name__ == "__main__":
    main()
