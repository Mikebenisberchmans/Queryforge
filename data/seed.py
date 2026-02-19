#!/usr/bin/env python3
"""
seed.py — Generates a realistic CRM SQLite database using a Snowflake Schema.
Run: python seed.py
Output: crm.db (SQLite database with 10,000+ records across many tables)

Snowflake Schema Overview:
─────────────────────────
FACT TABLE
  └── fact_sales

DIMENSION TABLES (first level)
  ├── dim_customer       → links to dim_geography, dim_segment
  ├── dim_product        → links to dim_category, dim_brand
  ├── dim_salesperson    → links to dim_department, dim_region
  ├── dim_date
  └── dim_channel

SUB-DIMENSION TABLES (second level — snowflake arms)
  ├── dim_geography      → links to dim_country
  ├── dim_segment
  ├── dim_category       → links to dim_subcategory
  ├── dim_brand
  ├── dim_department
  ├── dim_region
  ├── dim_country
  └── dim_subcategory

SUPPORTING CRM TABLES
  ├── crm_leads
  ├── crm_opportunities
  ├── crm_activities
  └── crm_support_tickets
"""

import sqlite3
import random
import string
from datetime import datetime, timedelta
from pathlib import Path

DB_PATH = Path("crm.db")
SEED = 42
random.seed(SEED)

# ─── Helpers ──────────────────────────────────────────────────────────────────

def rand_date(start: datetime, end: datetime) -> str:
    delta = end - start
    return (start + timedelta(seconds=random.randint(0, int(delta.total_seconds())))).strftime("%Y-%m-%d %H:%M:%S")

def rand_phone():
    return f"+1-{random.randint(200,999)}-{random.randint(100,999)}-{random.randint(1000,9999)}"

def rand_email(name: str, domain: str) -> str:
    clean = name.lower().replace(" ", ".").replace("'", "")
    return f"{clean}@{domain}"

def rand_str(n=8):
    return ''.join(random.choices(string.ascii_lowercase, k=n))

# ─── Reference data ───────────────────────────────────────────────────────────

COUNTRIES = ["United States", "United Kingdom", "Canada", "Australia", "Germany",
             "France", "India", "Brazil", "Japan", "Singapore"]

COUNTRY_CODES = {"United States": "US", "United Kingdom": "GB", "Canada": "CA",
                 "Australia": "AU", "Germany": "DE", "France": "FR", "India": "IN",
                 "Brazil": "BR", "Japan": "JP", "Singapore": "SG"}

REGIONS = ["North America", "Europe", "Asia Pacific", "Latin America", "Middle East & Africa"]

COUNTRY_REGION = {
    "United States": "North America", "Canada": "North America",
    "United Kingdom": "Europe", "Germany": "Europe", "France": "Europe",
    "India": "Asia Pacific", "Australia": "Asia Pacific", "Japan": "Asia Pacific",
    "Singapore": "Asia Pacific", "Brazil": "Latin America"
}

CITIES_BY_COUNTRY = {
    "United States": ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio", "San Diego"],
    "United Kingdom": ["London", "Manchester", "Birmingham", "Leeds", "Glasgow", "Liverpool"],
    "Canada": ["Toronto", "Vancouver", "Montreal", "Calgary", "Ottawa"],
    "Australia": ["Sydney", "Melbourne", "Brisbane", "Perth", "Adelaide"],
    "Germany": ["Berlin", "Munich", "Hamburg", "Frankfurt", "Cologne"],
    "France": ["Paris", "Lyon", "Marseille", "Toulouse", "Nice"],
    "India": ["Mumbai", "Delhi", "Bangalore", "Hyderabad", "Chennai", "Pune"],
    "Brazil": ["São Paulo", "Rio de Janeiro", "Brasília", "Salvador", "Fortaleza"],
    "Japan": ["Tokyo", "Osaka", "Yokohama", "Nagoya", "Sapporo"],
    "Singapore": ["Singapore"]
}

SEGMENTS = ["Enterprise", "Mid-Market", "SMB", "Startup", "Government", "Non-Profit"]
SEGMENT_DESC = {
    "Enterprise": "Large corporations with 1000+ employees",
    "Mid-Market": "Companies with 100-999 employees",
    "SMB": "Small and medium businesses under 100 employees",
    "Startup": "Early-stage companies under 5 years old",
    "Government": "Public sector and government entities",
    "Non-Profit": "Non-profit and charitable organizations"
}

CATEGORIES = {
    "Software": ["CRM Software", "ERP Software", "Analytics Tools", "Security Software", "Collaboration Tools"],
    "Hardware": ["Laptops", "Servers", "Networking Equipment", "Peripherals", "Storage Devices"],
    "Services": ["Consulting", "Implementation", "Training", "Support", "Managed Services"],
    "Cloud": ["IaaS", "PaaS", "SaaS", "Cloud Storage", "Cloud Security"]
}

BRANDS = ["TechPro", "NovaSoft", "CloudEdge", "DataSphere", "InnovateTech",
          "PeakSystems", "CoreLogic", "SkyBridge", "ApexTech", "NexGen"]

DEPARTMENTS = ["Inside Sales", "Field Sales", "Enterprise Sales", "Channel Sales", "Digital Sales"]

CHANNELS = [
    ("Direct", "Direct sales from sales team"),
    ("Online", "E-commerce and web portal"),
    ("Partner", "Reseller and partner network"),
    ("Phone", "Inbound and outbound calls"),
    ("Referral", "Customer referral program"),
]

LEAD_SOURCES = ["Website", "LinkedIn", "Cold Call", "Referral", "Trade Show",
                "Email Campaign", "Google Ads", "Partner", "Webinar", "Direct Mail"]

LEAD_STATUSES = ["New", "Contacted", "Qualified", "Unqualified", "Converted", "Lost"]

OPP_STAGES = ["Prospecting", "Qualification", "Proposal", "Negotiation", "Closed Won", "Closed Lost"]

ACTIVITY_TYPES = ["Call", "Email", "Meeting", "Demo", "Follow-up", "Proposal Sent", "Contract Sent"]

TICKET_STATUSES = ["Open", "In Progress", "Resolved", "Closed", "Escalated"]
TICKET_PRIORITIES = ["Low", "Medium", "High", "Critical"]

FIRST_NAMES = ["James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda",
               "William", "Barbara", "David", "Susan", "Richard", "Jessica", "Joseph", "Sarah",
               "Thomas", "Karen", "Charles", "Lisa", "Christopher", "Nancy", "Daniel", "Betty",
               "Matthew", "Margaret", "Anthony", "Sandra", "Mark", "Ashley", "Donald", "Dorothy",
               "Steven", "Kimberly", "Paul", "Emily", "Andrew", "Donna", "Joshua", "Michelle",
               "Priya", "Raj", "Aisha", "Mohammed", "Yuki", "Hiroshi", "Sofia", "Lucas", "Ana", "Pedro"]

LAST_NAMES = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
              "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
              "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson",
              "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson",
              "Patel", "Shah", "Sharma", "Kumar", "Tanaka", "Yamamoto", "Silva", "Santos",
              "Oliveira", "Müller", "Schmidt", "Fischer", "Weber", "Meyer", "Wagner"]

DOMAINS = ["gmail.com", "yahoo.com", "outlook.com", "company.com", "business.org",
           "corp.net", "enterprise.io", "techfirm.com", "globalcorp.com", "ventures.co"]

COMPANY_SUFFIXES = ["Inc.", "LLC", "Corp.", "Ltd.", "Group", "Solutions", "Technologies",
                    "Enterprises", "Holdings", "Partners", "Associates", "Consulting"]

COMPANY_ADJECTIVES = ["Global", "Advanced", "Premier", "Elite", "Dynamic", "Innovative",
                      "Strategic", "Integrated", "Digital", "Smart", "Peak", "Apex", "Core", "Next"]

COMPANY_NOUNS = ["Systems", "Networks", "Solutions", "Tech", "Data", "Cloud", "Ventures",
                 "Capital", "Media", "Logistics", "Finance", "Analytics", "Commerce", "Services"]


def rand_company():
    return f"{random.choice(COMPANY_ADJECTIVES)} {random.choice(COMPANY_NOUNS)} {random.choice(COMPANY_SUFFIXES)}"

def rand_name():
    return f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"


# ─── Main seeder ──────────────────────────────────────────────────────────────

def seed():
    if DB_PATH.exists():
        DB_PATH.unlink()
        print(f"🗑  Removed existing {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    cur = conn.cursor()

    print("📐 Creating schema...")

    # ── Sub-dimension: dim_country ─────────────────────────────────────────────
    cur.execute("""
        CREATE TABLE dim_country (
            country_id      INTEGER PRIMARY KEY AUTOINCREMENT,
            country_name    TEXT NOT NULL UNIQUE,
            country_code    TEXT NOT NULL,
            currency        TEXT NOT NULL,
            timezone        TEXT NOT NULL
        )
    """)

    # ── Sub-dimension: dim_region ──────────────────────────────────────────────
    cur.execute("""
        CREATE TABLE dim_region (
            region_id       INTEGER PRIMARY KEY AUTOINCREMENT,
            region_name     TEXT NOT NULL UNIQUE,
            region_head     TEXT NOT NULL
        )
    """)

    # ── Sub-dimension: dim_geography ──────────────────────────────────────────
    cur.execute("""
        CREATE TABLE dim_geography (
            geography_id    INTEGER PRIMARY KEY AUTOINCREMENT,
            city            TEXT NOT NULL,
            state_province  TEXT,
            postal_code     TEXT,
            country_id      INTEGER NOT NULL REFERENCES dim_country(country_id),
            region_id       INTEGER NOT NULL REFERENCES dim_region(region_id)
        )
    """)

    # ── Sub-dimension: dim_segment ────────────────────────────────────────────
    cur.execute("""
        CREATE TABLE dim_segment (
            segment_id      INTEGER PRIMARY KEY AUTOINCREMENT,
            segment_name    TEXT NOT NULL UNIQUE,
            description     TEXT,
            typical_deal_size TEXT
        )
    """)

    # ── Dimension: dim_customer ───────────────────────────────────────────────
    cur.execute("""
        CREATE TABLE dim_customer (
            customer_id     INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_name   TEXT NOT NULL,
            company_name    TEXT NOT NULL,
            email           TEXT NOT NULL UNIQUE,
            phone           TEXT,
            industry        TEXT,
            annual_revenue  REAL,
            employee_count  INTEGER,
            website         TEXT,
            geography_id    INTEGER NOT NULL REFERENCES dim_geography(geography_id),
            segment_id      INTEGER NOT NULL REFERENCES dim_segment(segment_id),
            created_at      TEXT NOT NULL,
            updated_at      TEXT NOT NULL,
            is_active       INTEGER DEFAULT 1
        )
    """)

    # ── Sub-dimension: dim_subcategory ────────────────────────────────────────
    cur.execute("""
        CREATE TABLE dim_subcategory (
            subcategory_id  INTEGER PRIMARY KEY AUTOINCREMENT,
            subcategory_name TEXT NOT NULL,
            description     TEXT
        )
    """)

    # ── Sub-dimension: dim_category ───────────────────────────────────────────
    cur.execute("""
        CREATE TABLE dim_category (
            category_id     INTEGER PRIMARY KEY AUTOINCREMENT,
            category_name   TEXT NOT NULL UNIQUE,
            subcategory_id  INTEGER NOT NULL REFERENCES dim_subcategory(subcategory_id),
            description     TEXT
        )
    """)

    # ── Sub-dimension: dim_brand ──────────────────────────────────────────────
    cur.execute("""
        CREATE TABLE dim_brand (
            brand_id        INTEGER PRIMARY KEY AUTOINCREMENT,
            brand_name      TEXT NOT NULL UNIQUE,
            brand_tier      TEXT NOT NULL,
            founded_year    INTEGER
        )
    """)

    # ── Dimension: dim_product ────────────────────────────────────────────────
    cur.execute("""
        CREATE TABLE dim_product (
            product_id      INTEGER PRIMARY KEY AUTOINCREMENT,
            product_name    TEXT NOT NULL,
            sku             TEXT NOT NULL UNIQUE,
            unit_price      REAL NOT NULL,
            cost_price      REAL NOT NULL,
            category_id     INTEGER NOT NULL REFERENCES dim_category(category_id),
            brand_id        INTEGER NOT NULL REFERENCES dim_brand(brand_id),
            is_active       INTEGER DEFAULT 1,
            launch_date     TEXT,
            description     TEXT
        )
    """)

    # ── Sub-dimension: dim_department ─────────────────────────────────────────
    cur.execute("""
        CREATE TABLE dim_department (
            department_id   INTEGER PRIMARY KEY AUTOINCREMENT,
            department_name TEXT NOT NULL UNIQUE,
            budget          REAL,
            head_of_dept    TEXT
        )
    """)

    # ── Dimension: dim_salesperson ────────────────────────────────────────────
    cur.execute("""
        CREATE TABLE dim_salesperson (
            salesperson_id  INTEGER PRIMARY KEY AUTOINCREMENT,
            full_name       TEXT NOT NULL,
            email           TEXT NOT NULL UNIQUE,
            phone           TEXT,
            hire_date       TEXT NOT NULL,
            quota           REAL NOT NULL,
            department_id   INTEGER NOT NULL REFERENCES dim_department(department_id),
            geography_id    INTEGER NOT NULL REFERENCES dim_geography(geography_id),
            manager_id      INTEGER REFERENCES dim_salesperson(salesperson_id),
            is_active       INTEGER DEFAULT 1
        )
    """)

    # ── Dimension: dim_date ───────────────────────────────────────────────────
    cur.execute("""
        CREATE TABLE dim_date (
            date_id         INTEGER PRIMARY KEY,
            full_date       TEXT NOT NULL UNIQUE,
            day             INTEGER NOT NULL,
            month           INTEGER NOT NULL,
            month_name      TEXT NOT NULL,
            quarter         INTEGER NOT NULL,
            year            INTEGER NOT NULL,
            week_of_year    INTEGER NOT NULL,
            day_of_week     INTEGER NOT NULL,
            day_name        TEXT NOT NULL,
            is_weekend      INTEGER NOT NULL,
            is_month_end    INTEGER NOT NULL,
            is_quarter_end  INTEGER NOT NULL
        )
    """)

    # ── Dimension: dim_channel ────────────────────────────────────────────────
    cur.execute("""
        CREATE TABLE dim_channel (
            channel_id      INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_name    TEXT NOT NULL UNIQUE,
            description     TEXT
        )
    """)

    # ── Fact: fact_sales ──────────────────────────────────────────────────────
    cur.execute("""
        CREATE TABLE fact_sales (
            sale_id         INTEGER PRIMARY KEY AUTOINCREMENT,
            date_id         INTEGER NOT NULL REFERENCES dim_date(date_id),
            customer_id     INTEGER NOT NULL REFERENCES dim_customer(customer_id),
            product_id      INTEGER NOT NULL REFERENCES dim_product(product_id),
            salesperson_id  INTEGER NOT NULL REFERENCES dim_salesperson(salesperson_id),
            channel_id      INTEGER NOT NULL REFERENCES dim_channel(channel_id),
            quantity        INTEGER NOT NULL,
            unit_price      REAL NOT NULL,
            discount_pct    REAL NOT NULL DEFAULT 0,
            gross_amount    REAL NOT NULL,
            discount_amount REAL NOT NULL,
            net_amount      REAL NOT NULL,
            cost_amount     REAL NOT NULL,
            gross_profit    REAL NOT NULL,
            tax_amount      REAL NOT NULL,
            total_amount    REAL NOT NULL,
            currency        TEXT NOT NULL DEFAULT 'USD',
            order_status    TEXT NOT NULL,
            payment_method  TEXT NOT NULL,
            invoice_number  TEXT NOT NULL UNIQUE,
            created_at      TEXT NOT NULL
        )
    """)

    # ── CRM: crm_leads ────────────────────────────────────────────────────────
    cur.execute("""
        CREATE TABLE crm_leads (
            lead_id         INTEGER PRIMARY KEY AUTOINCREMENT,
            lead_name       TEXT NOT NULL,
            company_name    TEXT NOT NULL,
            email           TEXT NOT NULL,
            phone           TEXT,
            source          TEXT NOT NULL,
            status          TEXT NOT NULL,
            geography_id    INTEGER REFERENCES dim_geography(geography_id),
            salesperson_id  INTEGER REFERENCES dim_salesperson(salesperson_id),
            estimated_value REAL,
            notes           TEXT,
            created_at      TEXT NOT NULL,
            updated_at      TEXT NOT NULL,
            converted_at    TEXT
        )
    """)

    # ── CRM: crm_opportunities ────────────────────────────────────────────────
    cur.execute("""
        CREATE TABLE crm_opportunities (
            opportunity_id  INTEGER PRIMARY KEY AUTOINCREMENT,
            lead_id         INTEGER REFERENCES crm_leads(lead_id),
            customer_id     INTEGER REFERENCES dim_customer(customer_id),
            salesperson_id  INTEGER NOT NULL REFERENCES dim_salesperson(salesperson_id),
            opportunity_name TEXT NOT NULL,
            stage           TEXT NOT NULL,
            probability     REAL NOT NULL,
            expected_value  REAL NOT NULL,
            actual_value    REAL,
            product_id      INTEGER REFERENCES dim_product(product_id),
            channel_id      INTEGER REFERENCES dim_channel(channel_id),
            expected_close  TEXT NOT NULL,
            actual_close    TEXT,
            created_at      TEXT NOT NULL,
            updated_at      TEXT NOT NULL
        )
    """)

    # ── CRM: crm_activities ───────────────────────────────────────────────────
    cur.execute("""
        CREATE TABLE crm_activities (
            activity_id     INTEGER PRIMARY KEY AUTOINCREMENT,
            activity_type   TEXT NOT NULL,
            salesperson_id  INTEGER NOT NULL REFERENCES dim_salesperson(salesperson_id),
            customer_id     INTEGER REFERENCES dim_customer(customer_id),
            lead_id         INTEGER REFERENCES crm_leads(lead_id),
            opportunity_id  INTEGER REFERENCES crm_opportunities(opportunity_id),
            subject         TEXT NOT NULL,
            outcome         TEXT,
            duration_mins   INTEGER,
            activity_date   TEXT NOT NULL,
            created_at      TEXT NOT NULL
        )
    """)

    # ── CRM: crm_support_tickets ──────────────────────────────────────────────
    cur.execute("""
        CREATE TABLE crm_support_tickets (
            ticket_id       INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_id     INTEGER NOT NULL REFERENCES dim_customer(customer_id),
            salesperson_id  INTEGER REFERENCES dim_salesperson(salesperson_id),
            product_id      INTEGER REFERENCES dim_product(product_id),
            ticket_number   TEXT NOT NULL UNIQUE,
            subject         TEXT NOT NULL,
            description     TEXT,
            status          TEXT NOT NULL,
            priority        TEXT NOT NULL,
            category        TEXT NOT NULL,
            resolution      TEXT,
            created_at      TEXT NOT NULL,
            updated_at      TEXT NOT NULL,
            resolved_at     TEXT
        )
    """)

    conn.commit()
    print("✅ Schema created.\n")

    # ─── Seed sub-dimensions ──────────────────────────────────────────────────

    print("🌱 Seeding sub-dimensions...")

    # dim_country
    currencies = {"US": "USD", "GB": "GBP", "CA": "CAD", "AU": "AUD", "DE": "EUR",
                  "FR": "EUR", "IN": "INR", "BR": "BRL", "JP": "JPY", "SG": "SGD"}
    timezones = {"US": "America/New_York", "GB": "Europe/London", "CA": "America/Toronto",
                 "AU": "Australia/Sydney", "DE": "Europe/Berlin", "FR": "Europe/Paris",
                 "IN": "Asia/Kolkata", "BR": "America/Sao_Paulo", "JP": "Asia/Tokyo", "SG": "Asia/Singapore"}

    for country in COUNTRIES:
        code = COUNTRY_CODES[country]
        cur.execute("INSERT INTO dim_country (country_name, country_code, currency, timezone) VALUES (?,?,?,?)",
                    (country, code, currencies[code], timezones[code]))

    # dim_region
    region_heads = {r: rand_name() for r in REGIONS}
    for r in REGIONS:
        cur.execute("INSERT INTO dim_region (region_name, region_head) VALUES (?,?)", (r, region_heads[r]))

    # dim_geography — generate cities
    country_ids = {row[0]: row[1] for row in cur.execute("SELECT country_name, country_id FROM dim_country")}
    region_ids = {row[0]: row[1] for row in cur.execute("SELECT region_name, region_id FROM dim_region")}

    geo_ids = []
    for country, cities in CITIES_BY_COUNTRY.items():
        for city in cities:
            cid = country_ids[country]
            rid = region_ids[COUNTRY_REGION[country]]
            cur.execute("INSERT INTO dim_geography (city, state_province, postal_code, country_id, region_id) VALUES (?,?,?,?,?)",
                        (city, f"State-{rand_str(4).upper()}", f"{random.randint(10000,99999)}", cid, rid))
            geo_ids.append(cur.lastrowid)

    # dim_segment
    deal_sizes = {"Enterprise": "$500K+", "Mid-Market": "$50K-$500K", "SMB": "$5K-$50K",
                  "Startup": "$1K-$10K", "Government": "$100K-$2M", "Non-Profit": "$5K-$100K"}
    for seg in SEGMENTS:
        cur.execute("INSERT INTO dim_segment (segment_name, description, typical_deal_size) VALUES (?,?,?)",
                    (seg, SEGMENT_DESC[seg], deal_sizes[seg]))

    # dim_subcategory
    all_subcats = []
    for cat, subcats in CATEGORIES.items():
        for sub in subcats:
            cur.execute("INSERT INTO dim_subcategory (subcategory_name, description) VALUES (?,?)",
                        (sub, f"{sub} products and offerings"))
            all_subcats.append((cur.lastrowid, cat))

    # dim_category
    subcat_map = {row[1]: row[0] for row in all_subcats}  # subcategory_name -> id (approximate)
    subcat_by_cat = {}
    for sid, cat in all_subcats:
        subcat_by_cat.setdefault(cat, []).append(sid)

    cat_ids = {}
    for cat, subcats in CATEGORIES.items():
        sid = subcat_by_cat[cat][0]
        cur.execute("INSERT INTO dim_category (category_name, subcategory_id, description) VALUES (?,?,?)",
                    (cat, sid, f"{cat} category"))
        cat_ids[cat] = cur.lastrowid

    # dim_brand
    tiers = ["Premium", "Standard", "Budget"]
    brand_ids = []
    for brand in BRANDS:
        cur.execute("INSERT INTO dim_brand (brand_name, brand_tier, founded_year) VALUES (?,?,?)",
                    (brand, random.choice(tiers), random.randint(1990, 2020)))
        brand_ids.append(cur.lastrowid)

    # dim_channel
    channel_ids = []
    for ch_name, ch_desc in CHANNELS:
        cur.execute("INSERT INTO dim_channel (channel_name, description) VALUES (?,?)", (ch_name, ch_desc))
        channel_ids.append(cur.lastrowid)

    # dim_department
    dept_ids = []
    for dept in DEPARTMENTS:
        head = rand_name()
        cur.execute("INSERT INTO dim_department (department_name, budget, head_of_dept) VALUES (?,?,?)",
                    (dept, round(random.uniform(500_000, 5_000_000), 2), head))
        dept_ids.append(cur.lastrowid)

    conn.commit()
    print("  ✅ Sub-dimensions seeded.\n")

    # ─── dim_date ─────────────────────────────────────────────────────────────

    print("📅 Seeding dim_date (2020–2025)...")
    start_date = datetime(2020, 1, 1)
    end_date = datetime(2025, 12, 31)
    months = ["January","February","March","April","May","June",
              "July","August","September","October","November","December"]
    days = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]

    d = start_date
    date_ids = {}
    while d <= end_date:
        date_id = int(d.strftime("%Y%m%d"))
        month_end = (d.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
        is_qend = d.month in [3,6,9,12] and d == month_end
        cur.execute("""INSERT INTO dim_date VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
            date_id, d.strftime("%Y-%m-%d"), d.day, d.month, months[d.month-1],
            (d.month - 1) // 3 + 1, d.year, int(d.strftime("%W")),
            d.weekday(), days[d.weekday()],
            1 if d.weekday() >= 5 else 0,
            1 if d == month_end else 0,
            1 if is_qend else 0
        ))
        date_ids[d.strftime("%Y-%m-%d")] = date_id
        d += timedelta(days=1)
    conn.commit()
    print(f"  ✅ {len(date_ids)} dates seeded.\n")

    # ─── dim_product ──────────────────────────────────────────────────────────

    print("📦 Seeding dim_products...")
    product_templates = {
        "Software": [
            ("CRM Pro", 299.99, 30.0), ("CRM Enterprise", 999.99, 100.0),
            ("ERP Suite", 1499.99, 150.0), ("Analytics Dashboard", 199.99, 20.0),
            ("Security Shield", 399.99, 40.0), ("Collab Hub", 149.99, 15.0),
        ],
        "Hardware": [
            ("ProBook Laptop 15", 1299.99, 800.0), ("Server Rack X1", 4999.99, 3000.0),
            ("NetSwitch 24-Port", 799.99, 400.0), ("Wireless AP Pro", 299.99, 150.0),
            ("SSD Array 10TB", 1999.99, 1200.0), ("USB-C Hub Elite", 89.99, 30.0),
        ],
        "Services": [
            ("Implementation Package", 5000.0, 2000.0), ("Training Bundle 5-Day", 3000.0, 500.0),
            ("Premium Support Annual", 2400.0, 400.0), ("Strategy Consulting (hr)", 250.0, 80.0),
            ("Managed Services Monthly", 1500.0, 600.0),
        ],
        "Cloud": [
            ("Cloud Starter 100GB", 49.99, 5.0), ("Cloud Business 1TB", 149.99, 15.0),
            ("Cloud Enterprise 10TB", 499.99, 50.0), ("Cloud Security Suite", 299.99, 30.0),
            ("PaaS Dev Environment", 199.99, 20.0),
        ]
    }

    product_ids = []
    for cat_name, products in product_templates.items():
        cid = cat_ids[cat_name]
        for p_name, price, cost in products:
            sku = f"SKU-{rand_str(3).upper()}-{random.randint(1000,9999)}"
            bid = random.choice(brand_ids)
            launch = rand_date(datetime(2018,1,1), datetime(2022,12,31))[:10]
            cur.execute("""INSERT INTO dim_product
                (product_name, sku, unit_price, cost_price, category_id, brand_id, is_active, launch_date, description)
                VALUES (?,?,?,?,?,?,?,?,?)""",
                (p_name, sku, price, cost, cid, bid, 1, launch, f"{p_name} - {cat_name} product"))
            product_ids.append((cur.lastrowid, price, cost))
    conn.commit()
    print(f"  ✅ {len(product_ids)} products seeded.\n")

    # ─── dim_salesperson ──────────────────────────────────────────────────────

    print("👤 Seeding dim_salesperson (50 reps)...")
    sp_ids = []
    managers = []
    for i in range(50):
        name = rand_name()
        email = rand_email(name, "salesteam.com")
        hire = rand_date(datetime(2015,1,1), datetime(2023,6,1))[:10]
        quota = round(random.uniform(200_000, 2_000_000), 2)
        dept_id = random.choice(dept_ids)
        geo_id = random.choice(geo_ids)
        manager_id = random.choice(managers) if managers and random.random() > 0.3 else None
        cur.execute("""INSERT INTO dim_salesperson
            (full_name, email, phone, hire_date, quota, department_id, geography_id, manager_id, is_active)
            VALUES (?,?,?,?,?,?,?,?,?)""",
            (name, email, rand_phone(), hire, quota, dept_id, geo_id, manager_id, 1))
        sp_ids.append(cur.lastrowid)
        if i < 10:
            managers.append(cur.lastrowid)
    conn.commit()
    print(f"  ✅ 50 salespersons seeded.\n")

    # ─── dim_customer ─────────────────────────────────────────────────────────

    print("🏢 Seeding dim_customer (1000 customers)...")
    seg_ids = [row[0] for row in cur.execute("SELECT segment_id FROM dim_segment")]
    industries = ["Technology", "Finance", "Healthcare", "Retail", "Manufacturing",
                  "Education", "Media", "Logistics", "Energy", "Real Estate"]
    customer_ids = []
    used_emails = set()
    for _ in range(1000):
        name = rand_name()
        email = rand_email(name, random.choice(DOMAINS))
        while email in used_emails:
            email = rand_email(name + rand_str(3), random.choice(DOMAINS))
        used_emails.add(email)
        company = rand_company()
        geo_id = random.choice(geo_ids)
        seg_id = random.choice(seg_ids)
        created = rand_date(datetime(2019,1,1), datetime(2024,1,1))
        updated = rand_date(datetime(2024,1,1), datetime(2025,6,1))
        cur.execute("""INSERT INTO dim_customer
            (customer_name, company_name, email, phone, industry, annual_revenue, employee_count,
             website, geography_id, segment_id, created_at, updated_at, is_active)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (name, company, email, rand_phone(), random.choice(industries),
             round(random.uniform(100_000, 500_000_000), 2),
             random.randint(5, 50000),
             f"www.{rand_str(8)}.com", geo_id, seg_id, created, updated, 1))
        customer_ids.append(cur.lastrowid)
    conn.commit()
    print(f"  ✅ 1000 customers seeded.\n")

    # ─── fact_sales ───────────────────────────────────────────────────────────

    print("💰 Seeding fact_sales (10,000 records)...")
    date_list = list(date_ids.items())  # [(date_str, date_id), ...]
    order_statuses = ["Completed", "Completed", "Completed", "Pending", "Cancelled", "Refunded"]
    payment_methods = ["Credit Card", "Bank Transfer", "PayPal", "Check", "Wire Transfer"]
    invoice_set = set()

    for i in range(10000):
        date_str, did = random.choice(date_list)
        cust_id = random.choice(customer_ids)
        prod_id, unit_price, cost_price = random.choice(product_ids)
        sp_id = random.choice(sp_ids)
        ch_id = random.choice(channel_ids)
        qty = random.randint(1, 50)
        discount = round(random.choice([0, 0, 0, 5, 10, 15, 20, 25]), 2)
        gross = round(unit_price * qty, 2)
        disc_amt = round(gross * discount / 100, 2)
        net = round(gross - disc_amt, 2)
        cost = round(cost_price * qty, 2)
        profit = round(net - cost, 2)
        tax = round(net * 0.08, 2)
        total = round(net + tax, 2)
        inv = f"INV-{random.randint(100000,999999)}"
        while inv in invoice_set:
            inv = f"INV-{random.randint(100000,999999)}"
        invoice_set.add(inv)

        cur.execute("""INSERT INTO fact_sales
            (date_id, customer_id, product_id, salesperson_id, channel_id, quantity,
             unit_price, discount_pct, gross_amount, discount_amount, net_amount, cost_amount,
             gross_profit, tax_amount, total_amount, currency, order_status, payment_method,
             invoice_number, created_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (did, cust_id, prod_id, sp_id, ch_id, qty, unit_price, discount,
             gross, disc_amt, net, cost, profit, tax, total, "USD",
             random.choice(order_statuses), random.choice(payment_methods),
             inv, date_str + " " + f"{random.randint(8,18):02d}:{random.randint(0,59):02d}:00"))

        if (i + 1) % 2000 == 0:
            conn.commit()
            print(f"    ... {i+1} sales committed")

    conn.commit()
    print(f"  ✅ 10,000 sales seeded.\n")

    # ─── crm_leads ────────────────────────────────────────────────────────────

    print("📋 Seeding crm_leads (2000 records)...")
    lead_ids = []
    for _ in range(2000):
        name = rand_name()
        created = rand_date(datetime(2020,1,1), datetime(2025,1,1))
        updated = rand_date(datetime(2025,1,1), datetime(2025,6,1))
        status = random.choice(LEAD_STATUSES)
        converted = rand_date(datetime(2025,1,1), datetime(2025,6,1)) if status == "Converted" else None
        cur.execute("""INSERT INTO crm_leads
            (lead_name, company_name, email, phone, source, status, geography_id,
             salesperson_id, estimated_value, notes, created_at, updated_at, converted_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (name, rand_company(), rand_email(name, random.choice(DOMAINS)),
             rand_phone(), random.choice(LEAD_SOURCES), status,
             random.choice(geo_ids), random.choice(sp_ids),
             round(random.uniform(1000, 500000), 2),
             f"Lead note {rand_str(12)}", created, updated, converted))
        lead_ids.append(cur.lastrowid)
    conn.commit()
    print(f"  ✅ 2000 leads seeded.\n")

    # ─── crm_opportunities ────────────────────────────────────────────────────

    print("🎯 Seeding crm_opportunities (1500 records)...")
    opp_ids = []
    for _ in range(1500):
        stage = random.choice(OPP_STAGES)
        prob = {"Prospecting": 10, "Qualification": 25, "Proposal": 50,
                "Negotiation": 75, "Closed Won": 100, "Closed Lost": 0}[stage]
        expected_val = round(random.uniform(5000, 1_000_000), 2)
        actual_val = round(expected_val * random.uniform(0.8, 1.2), 2) if stage in ["Closed Won", "Closed Lost"] else None
        created = rand_date(datetime(2021,1,1), datetime(2025,1,1))
        expected_close = rand_date(datetime(2025,1,1), datetime(2026,6,1))[:10]
        actual_close = rand_date(datetime(2024,6,1), datetime(2025,6,1))[:10] if stage in ["Closed Won", "Closed Lost"] else None
        cur.execute("""INSERT INTO crm_opportunities
            (lead_id, customer_id, salesperson_id, opportunity_name, stage, probability,
             expected_value, actual_value, product_id, channel_id, expected_close, actual_close, created_at, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (random.choice(lead_ids), random.choice(customer_ids),
             random.choice(sp_ids), f"Opportunity {rand_str(6).upper()}",
             stage, prob, expected_val, actual_val,
             random.choice(product_ids)[0], random.choice(channel_ids),
             expected_close, actual_close, created,
             rand_date(datetime(2025,1,1), datetime(2025,6,1))))
        opp_ids.append(cur.lastrowid)
    conn.commit()
    print(f"  ✅ 1500 opportunities seeded.\n")

    # ─── crm_activities ───────────────────────────────────────────────────────

    print("📞 Seeding crm_activities (3000 records)...")
    outcomes = ["Positive", "Neutral", "Negative", "No Answer", "Follow-up Required", "Converted"]
    for _ in range(3000):
        atype = random.choice(ACTIVITY_TYPES)
        activity_date = rand_date(datetime(2021,1,1), datetime(2025,6,1))
        cur.execute("""INSERT INTO crm_activities
            (activity_type, salesperson_id, customer_id, lead_id, opportunity_id,
             subject, outcome, duration_mins, activity_date, created_at)
            VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (atype, random.choice(sp_ids),
             random.choice(customer_ids) if random.random() > 0.3 else None,
             random.choice(lead_ids) if random.random() > 0.5 else None,
             random.choice(opp_ids) if random.random() > 0.4 else None,
             f"{atype} with {rand_name()}", random.choice(outcomes),
             random.randint(5, 120), activity_date, activity_date))
    conn.commit()
    print(f"  ✅ 3000 activities seeded.\n")

    # ─── crm_support_tickets ──────────────────────────────────────────────────

    print("🎫 Seeding crm_support_tickets (1500 records)...")
    ticket_categories = ["Billing", "Technical", "Account", "Feature Request", "Bug", "General"]
    ticket_set = set()
    for _ in range(1500):
        status = random.choice(TICKET_STATUSES)
        created = rand_date(datetime(2021,1,1), datetime(2025,1,1))
        updated = rand_date(datetime(2025,1,1), datetime(2025,6,1))
        resolved = rand_date(datetime(2025,1,1), datetime(2025,6,1)) if status in ["Resolved", "Closed"] else None
        tnum = f"TKT-{random.randint(10000,99999)}"
        while tnum in ticket_set:
            tnum = f"TKT-{random.randint(10000,99999)}"
        ticket_set.add(tnum)
        cur.execute("""INSERT INTO crm_support_tickets
            (customer_id, salesperson_id, product_id, ticket_number, subject, description,
             status, priority, category, resolution, created_at, updated_at, resolved_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (random.choice(customer_ids),
             random.choice(sp_ids) if random.random() > 0.3 else None,
             random.choice(product_ids)[0] if random.random() > 0.4 else None,
             tnum, f"Issue with {rand_str(6)} module",
             f"Customer reported: {rand_str(20)} is not working as expected.",
             status, random.choice(TICKET_PRIORITIES),
             random.choice(ticket_categories),
             f"Resolved by {rand_str(10)}" if resolved else None,
             created, updated, resolved))
    conn.commit()
    print(f"  ✅ 1500 support tickets seeded.\n")

    # ─── Indexes for performance ──────────────────────────────────────────────

    print("⚡ Creating indexes...")
    indexes = [
        "CREATE INDEX idx_fact_sales_date ON fact_sales(date_id)",
        "CREATE INDEX idx_fact_sales_customer ON fact_sales(customer_id)",
        "CREATE INDEX idx_fact_sales_product ON fact_sales(product_id)",
        "CREATE INDEX idx_fact_sales_salesperson ON fact_sales(salesperson_id)",
        "CREATE INDEX idx_fact_sales_channel ON fact_sales(channel_id)",
        "CREATE INDEX idx_customer_segment ON dim_customer(segment_id)",
        "CREATE INDEX idx_customer_geography ON dim_customer(geography_id)",
        "CREATE INDEX idx_product_category ON dim_product(category_id)",
        "CREATE INDEX idx_product_brand ON dim_product(brand_id)",
        "CREATE INDEX idx_leads_status ON crm_leads(status)",
        "CREATE INDEX idx_leads_salesperson ON crm_leads(salesperson_id)",
        "CREATE INDEX idx_opp_stage ON crm_opportunities(stage)",
        "CREATE INDEX idx_opp_salesperson ON crm_opportunities(salesperson_id)",
        "CREATE INDEX idx_activity_salesperson ON crm_activities(salesperson_id)",
        "CREATE INDEX idx_ticket_status ON crm_support_tickets(status)",
        "CREATE INDEX idx_ticket_customer ON crm_support_tickets(customer_id)",
    ]
    for idx in indexes:
        cur.execute(idx)
    conn.commit()
    print(f"  ✅ {len(indexes)} indexes created.\n")

    # ─── Summary ──────────────────────────────────────────────────────────────

    print("=" * 55)
    print("🎉 Database seeded successfully!")
    print(f"   File: {DB_PATH.resolve()}")
    print("=" * 55)
    tables = cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()
    total = 0
    for (t,) in tables:
        count = cur.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        total += count
        print(f"   {t:<30} {count:>6} rows")
    print(f"   {'TOTAL':<30} {total:>6} rows")
    print("=" * 55)

    conn.close()


if __name__ == "__main__":
    seed()
