import json
import psycopg2
import psycopg2.extras
import random
import time
import uuid
import threading
from datetime import datetime
from faker import Faker
from kafka import KafkaProducer
import logging
import sys
import os
from dotenv import load_dotenv

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

def load_config():
    with open('generator_config.json', 'r') as f:
        return json.load(f)

config = load_config()
fake = Faker()

# Attempt Kafka Producer Connection
try:
    producer = KafkaProducer(
        bootstrap_servers=config['kafka']['bootstrap_servers'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    logging.info("Connected to Kafka")
except Exception as e:
    logging.warning(f"Kafka not available ({e}). Clickstream will be printed to console.")
    producer = None

def get_db_connection():
    return psycopg2.connect(
        host=config['postgres'].get('host', 'localhost'),
        port=config['postgres'].get('port', 5433),
        user=os.environ.get('POSTGRES_USER', 'postgres'),
        password=os.environ.get('POSTGRES_PASSWORD', 'postgres_password'),
        dbname=os.environ.get('POSTGRES_DB', 'default_db')
    )

# --- 1. REFERENCE DATA INITIALIZATION ---
def seed_reference_data(conn):
    cur = conn.cursor()
    logging.info("Seeding reference data if empty...")
    
    # Check if regions exist
    cur.execute("SELECT COUNT(*) FROM regions;")
    if cur.fetchone()[0] == 0:
        regions = ['North', 'Central', 'South', 'Highlands']
        psycopg2.extras.execute_values(cur, "INSERT INTO regions (region_name) VALUES %s", [(r,) for r in regions])
        
    cur.execute("SELECT COUNT(*) FROM orderstatus;")
    if cur.fetchone()[0] == 0:
        psycopg2.extras.execute_values(cur, "INSERT INTO orderstatus (order_status_name) VALUES %s", [('Pending',), ('Processing',), ('Shipped',), ('Delivered',)])
        
    cur.execute("SELECT COUNT(*) FROM users;")
    if cur.fetchone()[0] == 0:
        users = [(fake.user_name()+str(i), 'pass', fake.email(), fake.phone_number()[:20]) for i in range(1, 101)]
        psycopg2.extras.execute_values(cur, "INSERT INTO users (username, password, email, mobile) VALUES %s", users)
        
    # Check if provinces exist
    cur.execute("SELECT COUNT(*) FROM provinces;")
    if cur.fetchone()[0] == 0:
        cur.execute("SELECT id FROM regions")
        r_ids = [row[0] for row in cur.fetchall()]
        provinces = []
        for i in range(20):
            provinces.append((fake.city(), random.choice(r_ids), fake.latitude(), fake.longitude()))
        psycopg2.extras.execute_values(cur, "INSERT INTO provinces (province_name, region_id, latitude, longitude) VALUES %s", provinces)

    # Categories
    cur.execute("SELECT COUNT(*) FROM categories;")
    if cur.fetchone()[0] == 0:
        cats = [('Electronics', 'electronics'), ('Home & Kitchen', 'home-kitchen'), ('Fashion', 'fashion'), ('Books', 'books')]
        psycopg2.extras.execute_values(cur, "INSERT INTO categories (category_name, slug) VALUES %s", cats)

    # Brands
    cur.execute("SELECT COUNT(*) FROM brands;")
    if cur.fetchone()[0] == 0:
        brands = [(fake.company(),) for _ in range(10)]
        psycopg2.extras.execute_values(cur, "INSERT INTO brands (brand_name) VALUES %s ON CONFLICT DO NOTHING", brands)
        
    # Tags
    cur.execute("SELECT COUNT(*) FROM tags;")
    if cur.fetchone()[0] == 0:
        tags = [(fake.unique.word(),) for _ in range(15)]
        psycopg2.extras.execute_values(cur, "INSERT INTO tags (tag_name) VALUES %s ON CONFLICT DO NOTHING", tags)
        
    # Products (Create the catalog)
    cur.execute("SELECT COUNT(*) FROM products;")
    if cur.fetchone()[0] == 0:
        cur.execute("SELECT id FROM categories")
        c_ids = [row[0] for row in cur.fetchall()]
        cur.execute("SELECT id FROM brands")
        b_ids = [row[0] for row in cur.fetchall()]
        
        products = []
        for i in range(200): # Seed 200 products
            cost = random.uniform(5.0, 500.0)
            price = cost * random.uniform(1.2, 2.5)
            products.append((
                fake.catch_phrase(), random.choice(c_ids), random.choice(b_ids),
                fake.text(max_nb_chars=100), round(price, 2), round(cost, 2),
                0.08, random.randint(10, 1000)
            ))
        psycopg2.extras.execute_values(cur, 
            "INSERT INTO products (product_name, category_id, brand_id, product_description, product_price, unit_cost, product_tax, product_quantity) VALUES %s", 
            products)
            
    conn.commit()
    cur.close()

# --- 2. STATISTICAL SIMULATION UTILS ---
def get_zipfian_product(product_list):
    # Zipfian (Power Law): rank 1 is selected highly often.
    # We use pareto distribution as a proxy for Zipfian continuous sampling.
    rank = int(random.paretovariate(1.16)) # Standard parameter
    if rank >= len(product_list):
        rank = random.randint(0, len(product_list)-1) # fallback
    return product_list[rank]

def normal_latency(mean_sec, std_sec):
    # Gaussian sleep to simulate realistic network/system slowness
    sleep_time = random.gauss(mean_sec, std_sec)
    time.sleep(max(0.1, sleep_time)) # Floor latency at 0.1s

    # --- 3. SESSION SIMULATION THREAD ---
def simulate_session(user_id, product_list, c_ids, r_ids, b_ids, o_status_id):
    session_id = str(uuid.uuid4())
    
    # State flags
    cart = []
    
    # Behavioral Markov Chain steps
    ACTIONS = config['simulation']['probabilities']
    
    def emit_click(event_name, properties=None):
        payload = {
            "event_id": str(uuid.uuid4()),
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "session_id": session_id,
            "user_id": user_id,
            "event_name": event_name,
            "properties": properties or {}
        }
        if producer:
            producer.send(config['kafka']['topic_clickstream'], payload)
            
        uid_str = user_id if user_id else "anon"
        logging.info(f"[Session {session_id[:8]}] User {uid_str} performed '{event_name}' - {properties or {}}")
        
    # Session start
    emit_click("session_start")
    
    # Usually users browse a bit
    num_events = random.randint(*config['simulation']['events_per_session_range'])
    
    for _ in range(num_events):
        normal_latency(1.0, 0.5) # Time spent viewing the last action
        
        # Determine next action randomly based on weights
        actions, weights = zip(*ACTIONS.items())
        action = random.choices(actions, weights=weights)[0]
        
        if action == "page_view":
            emit_click("page_view", {"url": f"/category/{random.choice(c_ids)}"})
        elif action == "search":
            emit_click("search", {"query": fake.word()})
        elif action == "view_item":
            prod = get_zipfian_product(product_list)
            emit_click("view_item", {"product_id": prod['id']})
        elif action == "add_to_cart":
            prod = get_zipfian_product(product_list)
            cart.append(prod)
            emit_click("add_to_cart", {"product_id": prod['id'], "price": float(prod['price'])})
        elif action == "remove_from_cart":
            if cart:
                removed = cart.pop()
                emit_click("remove_from_cart", {"product_id": removed['id']})
        elif action == "checkout":
            if cart:
                emit_click("begin_checkout", {"cart_value": sum([float(p['price']) for p in cart])})
        elif action == "purchase" and cart:
            try:
                # Perform ACID write to database
                conn = get_db_connection()
                cur = conn.cursor()
                
                # Check if user exists, if not, wait we use random users. Let's lazily create user if needed.
                if not user_id:
                    # Anonymous checkout
                    username = fake.user_name() + str(random.randint(1000,9999))
                    cur.execute("INSERT INTO users (username, password, email, mobile) VALUES (%s, %s, %s, %s) RETURNING id",
                               (username, 'pass', fake.email(), fake.phone_number()[:20]))
                    user_id = cur.fetchone()[0]
                    # Create generic address
                    cur.execute("SELECT id, region_id FROM provinces LIMIT 1")
                    prov = cur.fetchone()
                    cur.execute("INSERT INTO addresses (title, user_id, region_id, province_id, full_address) VALUES (%s, %s, %s, %s, %s) RETURNING id",
                               ('Home', user_id, prov[1], prov[0], fake.address()[:250]))
                    addr_id = cur.fetchone()[0]
                else:
                    cur.execute("SELECT id FROM addresses WHERE user_id = %s LIMIT 1", (user_id,))
                    addr_res = cur.fetchone()
                    if not addr_res:
                         cur.execute("SELECT id, region_id FROM provinces LIMIT 1")
                         prov = cur.fetchone()
                         cur.execute("INSERT INTO addresses (title, user_id, region_id, province_id, full_address) VALUES (%s, %s, %s, %s, %s) RETURNING id",
                               ('Home', user_id, prov[1], prov[0], fake.address()[:250]))
                         addr_id = cur.fetchone()[0]
                    else:
                        addr_id = addr_res[0]

                # Insert Order
                total = sum([float(p['price']) for p in cart])
                cur.execute("INSERT INTO orders (user_id, staff_id, address_id, order_amount, discount_amount, tax_amount, total_amount, order_status_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id",
                           (user_id, user_id, addr_id, total, 0, sum([float(p['price'])*0.08 for p in cart]), total*1.08, o_status_id))
                order_id = cur.fetchone()[0]
                
                # Insert Order Details
                for p in cart:
                    price_val = float(p['price'])
                    cur.execute("INSERT INTO orderdetails (order_id, product_id, quantity, product_price, product_tax, subtotal_amount) VALUES (%s, %s, %s, %s, %s, %s)",
                               (order_id, p['id'], 1, price_val, price_val*0.08, price_val*1.08))
                
                conn.commit()
                cur.close()
                conn.close()
                
                logging.info(f"Successfully processed purchase transaction for order_id: {order_id}")
                emit_click("purchase", {"order_id": order_id, "total": total*1.08})
                cart.clear() # Success
                break # End session after purchase
            except Exception as e:
                logging.exception(f"Error executing transaction: {e}")
                if 'conn' in locals() and conn: conn.rollback()
    
    emit_click("session_end")

# --- 4. MASTER LOOP ---
def run_simulation():
    try:
        conn = get_db_connection()
    except Exception as e:
        logging.error(f"Database not ready yet... {e}")
        return
        
    seed_reference_data(conn)
    logging.info("Loading catalog into memory for fast Zipfian access...")
    cur = conn.cursor()
    cur.execute("SELECT id, product_price FROM products")
    catalog_res = cur.fetchall()
    product_list = [{'id': row[0], 'price': row[1]} for row in catalog_res]
    
    cur.execute("SELECT id FROM categories")
    c_ids = [row[0] for row in cur.fetchall()]
    
    cur.execute("SELECT id FROM regions")
    r_ids = [row[0] for row in cur.fetchall()]

    cur.execute("SELECT id FROM brands")
    b_ids = [row[0] for row in cur.fetchall()]
    
    cur.execute("SELECT id FROM users")
    u_ids = [row[0] for row in cur.fetchall()]
    
    cur.execute("SELECT id FROM orderstatus ORDER BY id ASC LIMIT 1")
    res = cur.fetchone()
    o_status_id = res[0] if res else 1
    
    cur.close()
    conn.close()

    logging.info("--- REAL-TIME SIMULATION ENGINE STARTED ---")
    logging.info("Using Poisson distribution for arrival rates and Zipfian for product popularity.")
    
    # Lambda for Poisson arrival: 1 user every X seconds on average
    rate_lambda = 0.5 
    
    try:
        while True:
            # Poisson arrival time calculation
            inter_arrival_time = random.expovariate(rate_lambda)
            time.sleep(inter_arrival_time)
            
            # Start a thread to handle this user's session without blocking the master loop
            # Simulate an anonymous user mostly, or registered 
            is_registered = random.random() < 0.3
            uid = random.choice(u_ids) if is_registered and u_ids else None
            
            t = threading.Thread(target=simulate_session, args=(uid, product_list, c_ids, r_ids, b_ids, o_status_id))
            t.daemon = True
            t.start()
            logging.info(f"Started simulation session thread for user {'anon' if uid is None else uid}")
            
    except KeyboardInterrupt:
        logging.info("\nStopping Simulator...")
        if producer: producer.flush()

if __name__ == '__main__':
    run_simulation()
