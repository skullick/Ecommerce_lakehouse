"""
Catalog and configuration data for e-commerce data generation.

This module contains all fixed statistics, distributions, and catalog data
used throughout the model generation process. Centralizing these makes the
system more configurable and maintainable.
"""

from collections import OrderedDict

# ============================================================================
# USER GENERATION STATISTICS
# ============================================================================

# Age distribution for customer generation
# Default age ranges and their relative probabilities
AGE_DISTRIBUTION = OrderedDict([
    ("18-24", 0.20),
    ("25-34", 0.35),
    ("35-44", 0.25),
    ("45-54", 0.15),
    ("55-70", 0.05),
])

# Traffic source distribution for customer acquisition
# Realistic e-commerce traffic sources and their probability
TRAFFIC_SOURCES = OrderedDict([
    ("Organic", 0.15),
    ("Facebook", 0.06),
    ("Search", 0.70),
    ("Email", 0.05),
    ("Display", 0.04),
])

# Probability for a customer to be logged in (0.0 to 1.0)
LOGGED_IN_PROB = 0.7


# ============================================================================
# ADDRESS GENERATION STATISTICS
# ============================================================================

# Address type distribution (home vs office)
ADDRESS_TYPE_DISTRIBUTION = OrderedDict([
    ("home", 0.70),
    ("office", 0.30),
])

# ============================================================================
# EVENT GENERATION STATISTICS
# ============================================================================

# Browser distribution for event tracking
BROWSER_DISTRIBUTION = OrderedDict([
    ("IE", 0.05),
    ("Edge", 0.10),
    ("Chrome", 0.45),
    ("Safari", 0.20),
    ("Firefox", 0.15),
    ("Other", 0.05),
])

# Device distribution for event tracking
DEVICE_DISTRIBUTION = OrderedDict([
    ("desktop", 0.40),
    ("mobile", 0.50),
    ("tablet", 0.10),
])

# Traffic source distribution for events
# Different from user acquisition (more email marketing driven)
EVENT_TRAFFIC_SOURCES = OrderedDict([
    ("Email", 0.45),
    ("Adwords", 0.30),
    ("Organic", 0.05),
    ("YouTube", 0.10),
    ("Facebook", 0.10),
])

# Mapping of utm_source to utm_medium for consistent tracking
UTM_SOURCE_TO_MEDIUM = {
    "Email": "email",
    "Adwords": "cpc",
    "Organic": "organic",
    "YouTube": "cpc",
    "Facebook": "cpc",
}

# Navigation Graph (Markov Chain probabilities for state transitions)
# Must sum to 1.0 for each state. Defines the probability of the NEXT action.
# Actions can be state changes (e.g. 'category') or in-state actions (e.g. 'add_to_cart').
NAVIGATION_GRAPH = {
    "home": {
        "category": 0.50,
        "item": 0.30,
        "exit": 0.20
    },
    "category": {
        "item": 0.60,
        "home": 0.15,
        "exit": 0.25
    },
    "item": {
        "add_to_cart": 0.30,   # Conditional: Stays on 'item'
        "cart": 0.20,
        "category": 0.20,
        "home": 0.10,
        "exit": 0.20
    },
    "cart": {
        "checkout": 0.50,
        "remove_from_cart": 0.10, # Conditional: Stays on 'cart'
        "item": 0.20,
        "exit": 0.20
    },
    "checkout": {
        "purchase": 0.70,      # Conditional: Ends session
        "cart": 0.10,
        "home": 0.10,
        "exit": 0.10
    }
}

# Configurable delays between steps in a user journey (in seconds)
# Can be tweaked to speed up simulation or make it perfectly realistic
EVENT_TIMING = {
    "page_view_delay_min": 1,
    "page_view_delay_max": 5,
    "product_view_delay_min": 2,
    "product_view_delay_max": 8,
    "add_to_cart_delay_min": 1,
    "add_to_cart_delay_max": 3,
    "checkout_delay_min": 2,
    "checkout_delay_max": 10
}

# ============================================================================
# ORDER GENERATION STATISTICS
# ============================================================================


# Shipping method distribution (Standard vs Express)
SHIPPING_METHOD_DISTRIBUTION = OrderedDict([
    (1, 0.70),  # Standard Shipping
    (2, 0.30),  # Express Shipping
])

# Discount application probability (realistic e-commerce data: ~30% use discounts)
DISCOUNT_APPLICATION_PROBABILITY = 0.30

# Transaction lifecycle timing configuration.
# All delay values are in seconds (used with random.randint for simulation realism).
TRANSACTION_TIMING = {
    # Online payment: created shortly after order placement (30 sec – 5 min)
    "online_payment_delay_min_s":  30,
    "online_payment_delay_max_s":  300,

    # Online payment failure rate (e.g. 2% of online transactions fail)
    "online_payment_failure_rate": 0.02,

    # COD payment: created when the order is delivered; add a small processing offset
    "cod_payment_delay_min_s":     0,
    "cod_payment_delay_max_s":     60,

    # Refund: created after the order is marked returned (1 min – 30 min)
    "refund_delay_min_s":          60,
    "refund_delay_max_s":          1800,

    # Order status transitions:
    # Processing -> Shipped (30 min - 4 hours) - reduced for demo: (10 sec - 1 min)
    "order_shipped_delay_min_s":   10,
    "order_shipped_delay_max_s":   60,

    # Shipped -> Delivered (1 day - 5 days) - reduced for demo: (30 sec - 2 min)
    "order_delivered_delay_min_s": 30,
    "order_delivered_delay_max_s": 120,
}

#
ORDER_RETURN_RATE = 0.1
ORDER_CANCEL_RATE = 0.05

# ============================================================================
# PRODUCT CATALOG
# ============================================================================

# Product categories and their subcategories
# Represents realistic e-commerce product hierarchy
PRODUCT_CATEGORIES = {
    "Consumer Electronics": [
        "Smartphones",
        "Laptops",
        "Tablets & eReaders",
        "Desktop Computers",
        "Smartwatches & Fitness Bands",
        "Portable Media Players",
    ],
    "Home Entertainment": [
        "Televisions (LED, OLED, QLED)",
        "Soundbars & Home Theater Systems",
        "Streaming Devices",
        "Projectors",
        "Blu-ray & DVD Players",
        "Gaming Consoles",
    ],
    "Kitchen Appliances": [
        "Refrigerators",
        "Microwaves",
        "Ovens & Ranges",
        "Dishwashers",
        "Coffee Machines",
        "Blenders & Food Processors",
        "Air Fryers",
        "Electric Kettles",
    ],
    "Laundry & Cleaning": [
        "Washing Machines",
        "Dryers",
        "Vacuum Cleaners",
        "Robot Vacuums",
        "Steam Cleaners",
        "Garment Steamers",
    ],
    "Heating, Cooling & Air Quality": [
        "Air Conditioners",
        "Air Purifiers",
        "Humidifiers & Dehumidifiers",
        "Electric Fans",
        "Heaters",
        "Smart Thermostats",
    ],
    "Smart Home & IoT": [
        "Smart Lighting",
        "Smart Speakers",
        "Smart Security Cameras",
        "Smart Door Locks",
        "Smart Plugs & Switches",
        "Home Automation Hubs",
    ],
    "Personal Care Electronics": [
        "Hair Dryers & Stylers",
        "Electric Shavers & Trimmers",
        "Electric Toothbrushes",
        "Skin Care Devices",
        "Massagers",
    ],
    "Office & Productivity": [
        "Printers & Scanners",
        "Monitors",
        "Keyboards & Mice",
        "Networking Equipment (Routers & Modems)",
        "External Storage Devices",
    ],
    "Outdoor & Garden Tech": [
        "Electric Lawn Mowers",
        "Smart Irrigation Systems",
        "Outdoor Lighting",
        "Weather Stations",
        "Pool Cleaning Robots",
    ],
    "Energy & Power Solutions": [
        "Solar Panels",
        "Home Battery Storage",
        "Portable Power Stations",
        "UPS (Uninterruptible Power Supply)",
        "Electric Vehicle Chargers",
    ],
}

# Brand assignments by product category
# Ensures realistic brand-to-category mapping
BRANDS_BY_CATEGORY = {
    "Consumer Electronics": [
        "Apple",
        "Samsung",
        "Xiaomi",
        "Google",
        "OnePlus",
        "Motorola",
        "Sony",
        "LG",
    ],
    "Home Entertainment": [
        "Samsung",
        "LG",
        "Sony",
        "Panasonic",
        "Philips",
        "TCL",
        "Roku",
        "Amazon",
        "Bose",
    ],
    "Kitchen Appliances": [
        "Philips",
        "Whirlpool",
        "Samsung",
        "LG",
        "Bosch",
        "Midea",
        "Electrolux",
        "Instant Pot",
        "Ninja",
    ],
    "Laundry & Cleaning": [
        "Samsung",
        "LG",
        "Whirlpool",
        "Bosch",
        "Dyson",
        "Hoover",
        "Bissell",
        "iRobot",
        "Shark",
    ],
    "Heating, Cooling & Air Quality": [
        "Daikin",
        "Midea",
        "Gree",
        "Panasonic",
        "Sharp",
        "Philips",
        "Levoit",
        "Dyson",
        "Honeywell",
    ],
    "Smart Home & IoT": [
        "Amazon",
        "Google",
        "Philips",
        "LIFX",
        "Yale",
        "Arlo",
        "Ring",
        "TP-Link",
        "Wyze",
    ],
    "Personal Care Electronics": [
        "Dyson",
        "Philips",
        "Braun",
        "Panasonic",
        "Oral-B",
        "Sonicare",
        "Beurer",
        "NuFace",
    ],
    "Office & Productivity": [
        "HP",
        "Canon",
        "Brother",
        "Dell",
        "LG",
        "ASUS",
        "Razer",
        "Corsair",
        "Western Digital",
    ],
    "Outdoor & Garden Tech": [
        "Worx",
        "Greenworks",
        "Husqvarna",
        "Rainbird",
        "Hunter",
        "Kasa",
        "Ecovacs",
    ],
    "Energy & Power Solutions": [
        "Tesla",
        "Sunpower",
        "Anker",
        "Goal Zero",
        "EcoFlow",
        "Greenpacket",
        "Delta",
    ],
}

# Product name templates by subcategory
# Used to generate realistic product names
PRODUCT_TEMPLATES = {
    "Smartphones": [
        "{brand} {series} {model}",
        "{brand} {series} Pro Max",
        "{brand} {series} Ultra",
    ],
    "Laptops": [
        "{brand} {series} {model}GB RAM",
        "{brand} {series} {model}\" Display",
        "{brand} {series} {spec} Edition",
    ],
    "Tablets & eReaders": [
        "{brand} {series} {model}\"",
        "{brand} E-Reader {model}GB",
    ],
    "Televisions (LED, OLED, QLED)": [
        "{brand} {model}\" {spec} TV",
        "{brand} Smart TV {model}P {spec}",
    ],
    "Air Conditioners": [
        "{brand} {model} Ton {spec}",
        "{brand} Smart AC {model}K",
    ],
    "Washing Machines": [
        "{brand} {model}Kg Fully Automatic",
        "{brand} Front Load {model}Kg",
    ],
    "Smart Lighting": [
        "{brand} Smart Bulb {model}W",
        "{brand} Color Smart Light {spec}",
    ],
    "Coffee Machines": [
        "{brand} {spec} Coffee Maker",
        "{brand} {model}L Coffee Machine",
    ],
    "Vacuum Cleaners": [
        "{brand} {spec} Vacuum Cleaner",
        "{brand} Cordless Vacuum {model}Ah",
    ],
    "Electric Toothbrushes": [
        "{brand} Electric Toothbrush {model}k RPM",
        "{brand} Smart Toothbrush {spec}",
    ],
    "Printers & Scanners": [
        "{brand} {spec} {model}ppm Printer",
        "{brand} Multifunction Printer {model}",
    ],
    "Smart Speakers": [
        "{brand} Smart Speaker {spec}",
        "{brand} {model}\" Display Speaker",
    ],
    "Refrigerators": [
        "{brand} {model}L {spec} Refrigerator",
        "{brand} French Door {model}L",
    ],
    "Microwaves": [
        "{brand} {model}W Microwave Oven",
        "{brand} {spec} Microwave {model}L",
    ],
    "Air Purifiers": [
        "{brand} Air Purifier {spec} Area",
        "{brand} {model} Air Purifier",
    ],
    "Robot Vacuums": [
        "{brand} Robot Vacuum {spec}",
        "{brand} Smart Robotic Cleaner {model}",
    ],
}

# Product specifications by subcategory
# Used to add technical details to product names
PRODUCT_SPECS = {
    "Smartphones": ["128GB", "256GB", "512GB", "1TB"],
    "Laptops": ["Intel i5", "Intel i7", "AMD Ryzen 5", "AMD Ryzen 7", "M1", "M2"],
    "Televisions (LED, OLED, QLED)": ["4K", "8K", "OLED", "QLED", "Mini LED"],
    "Air Conditioners": ["1.5", "2", "2.5", "3", "3.5"],
    "Washing Machines": ["6", "7", "8", "9", "10", "12"],
    "Smart Lighting": ["16M Colors", "Dimmable", "WiFi", "Zigbee"],
    "Coffee Machines": ["Espresso", "Drip", "Capsule", "Pour Over"],
    "Vacuum Cleaners": ["Cordless", "Upright", "Robot", "Handheld"],
    "Electric Toothbrushes": ["Smart", "Sonic", "Oscillating", "UV Clean"],
    "Air Purifiers": ["HEPA", "Smart WiFi", "Ionizer", "Carbon Filter"],
    "Refrigerators": ["French Door", "Side-by-Side", "Top Freezer", "Bottom Freezer"],
    "Microwaves": ["Convection", "Smart", "Grill", "Solo"],
    "Robot Vacuums": ["WiFi Enabled", "2D Mapping", "3D Lidar", "Smart Navigation"],
}

PRODUCT_COST_RANGE = {
    "Smartphones": (300, 800),
    "Laptops": (500, 1500),
    "Tablets & eReaders": (200, 700),
    "Desktop Computers": (400, 1200),
    "Televisions (LED, OLED, QLED)": (300, 1500),
    "Air Conditioners": (200, 800),
    "Washing Machines": (300, 1000),
    "Refrigerators": (400, 1200),
    "Smart Lighting": (10, 50),
    "Coffee Machines": (50, 300),
    "Vacuum Cleaners": (100, 600),
    "Electric Toothbrushes": (30, 150),
    "Air Purifiers": (100, 400),
    "Robot Vacuums": (200, 800),
    "Printers & Scanners": (100, 500),
    "Smart Speakers": (50, 200),
    "Microwaves": (80, 400),
    "Keyboards & Mice": (20, 150),
    "Monitors": (150, 800),
    "Soundbars & Home Theater Systems": (100, 800),
}

# ============================================================================
# DISCOUNT GENERATION STATISTICS
# ============================================================================
# Percentage and absolute discount ranges for promotions
MIN_PERCENT_DISCOUNT = 1.0        # 1%
MAX_PERCENT_DISCOUNT = 75.0       # 75%
MIN_AMOUNT_DISCOUNT = 5.0         # $5
MAX_AMOUNT_DISCOUNT = 1000.0      # $1000
DISCOUNT_APPLY_PROB = 0.3

# Campaign names for advertising simulations
CAMPAIGN_SUFFIXES = [
    "Sale",
    "Discounts",
    "Promo",
    "Deals",
    "Special",
    "Gift",
    "Roundup",
    "Rewards",
    "Bonus Program",
]

# ============================================================================
# CAMPAIGN TYPE DISTRIBUTIONS
# ============================================================================
# Each campaign type carries a probability weight and a realistic duration band.
# The distribution is drawn from real e-commerce marketing calendars:
#   - Flash sales (~20%): short, high-urgency windows (2–48 h)
#   - Weekend sales (~20%): Fri–Sun promotions (48–72 h)
#   - Weekly promos (~30%): the most common promotional cadence (4–7 days)
#   - Seasonal events (~20%): major holidays, 1–3 weeks
#   - Loyalty/member programs (~10%): long-running, 2–4 weeks
#
# Override via config key: "promotions.campaign_types"
CAMPAIGN_TYPES = {
    "flash":    {"weight": 0.20, "min_hours":   1, "max_hours":   48},
    "weekend":  {"weight": 0.20, "min_hours":  48, "max_hours":   72},
    "weekly":   {"weight": 0.30, "min_hours":  96, "max_hours":  168},
    "seasonal": {"weight": 0.20, "min_hours": 168, "max_hours":  504},
    "loyalty":  {"weight": 0.10, "min_hours": 336, "max_hours":  720},
}

# ============================================================================
# DISCOUNT ARCHETYPE PROBABILITIES BY CAMPAIGN TYPE
# ============================================================================
# Longer campaigns favour staged/partial rollouts (loyalty rewards, early-bird);
# short flash campaigns favour short burst coupons.
# Archetypes: "flash" = burst coupon, "full" = runs whole campaign, "partial" = delayed start.
#
# Override via config key: "promotions.discount_archetypes"
DISCOUNT_ARCHETYPES_BY_CAMPAIGN = {
    #               flash   full   partial
    "flash":    {"flash": 1.00, "full": 0, "partial": 0},
    "weekend":  {"flash": 0.20, "full": 0.55, "partial": 0.25},
    "weekly":   {"flash": 0.15, "full": 0.40, "partial": 0.45},
    "seasonal": {"flash": 0.10, "full": 0.30, "partial": 0.60},
    "loyalty":  {"flash": 0.05, "full": 0.20, "partial": 0.75},
}

# ============================================================================
# FLASH DISCOUNT SCHEDULING (peak launch hours)
# ============================================================================
# Flash discounts are snapped to realistic peak-traffic hours rather than
# launching at arbitrary times. Based on e-commerce traffic studies:
#   Weekdays: lunch peak (12–14h) and evening peak (18–21h) dominate.
#   Weekends: traffic is more spread across mid-morning through evening.
#
# Override via config key: "promotions.flash_peak_hours"
PEAK_HOURS = {
    "weekday": [9, 12, 18, 21],   # Mon–Fri: morning, lunch, evening, late-night
    "weekend": [10, 12, 15, 19],  # Sat–Sun: mid-morning, noon, afternoon, evening
}

# Relative weight for each peak hour slot (must align with FLASH_DISCOUNT_PEAK_HOURS).
# Higher weight → more likely to be chosen for the flash launch time.
#
# Override via config key: "promotions.flash_hour_weights"
FLASH_DISCOUNT_HOUR_WEIGHTS = {
    "weekday": [0.15, 0.30, 0.35, 0.20],  # Evening dominates on weekdays
    "weekend": [0.20, 0.30, 0.25, 0.25],  # More evenly spread on weekends
}
