import argparse
import asyncio
import csv
import os
import random
import logging
import datetime
import time
import uuid
from collections import OrderedDict

from dotenv import load_dotenv
from faker import Faker
from sqlalchemy.exc import SQLAlchemyError, OperationalError

from data.db_writer import DataWriter
from data.event_producer import EventProducer
from data.models import (
    User,
    Order,
    OrderItem,
    OrderStatusHistory,
    Adscampaign,
    Discount,
    Address,
    Brand,
    Category,
    Product,
    Transaction,
)
from data.constants import (
    OrderStatus,
    ShippingStatus,
    PaymentMethod,
    PaymentStatus,
    TransactionType,
)
from data.catalog import (
    PRODUCT_CATEGORIES,
    BRANDS_BY_CATEGORY,
    TRANSACTION_TIMING,
    DISCOUNT_APPLY_PROB,
    ORDER_RETURN_RATE,
    ORDER_CANCEL_RATE,
    BROWSER_DISTRIBUTION,
    EVENT_TRAFFIC_SOURCES,
    LOGGED_IN_PROB
)

logging.basicConfig(
    level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s"
)


class ECommSimulator:
    """
    Manages the state and execution of the e-commerce data generation simulation.
    """

    def __init__(self, args: argparse.Namespace, fake: Faker, config: dict = None):
        """Initializes the simulator and the DataWriter."""
        self.args = args
        self.fake = fake
        self.config = config or {}
        self.writer = DataWriter(
            args.db_user,
            args.db_password,
            args.db_host,
            args.db_name,
            args.db_schema,
            args.db_batch_size,
        )
        self.consecutive_db_errors = 0
        self.max_consecutive_errors = 3
        self._txn_timing = TRANSACTION_TIMING  # central timing config
        
        # Initialize Event Producer for clickstream telemetry
        kafka_broker = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.event_producer = EventProducer(bootstrap_servers=kafka_broker)
        
        # Internal Queue for tracking active orders
        self.active_orders_queue = asyncio.Queue()
        
        # Concurrency safety: Set of currently busy customer IDs
        self.busy_customers = set()

    def _create_payment_transaction(
        self,
        order: Order,
        is_cod: bool = False,
    ) -> Transaction:
        """
        Build a payment Transaction for the given order.

        Online payments have a small chance of failure (configured in catalog).
        COD payments always succeed (cash is tendered on delivery).
        """
        t = self._txn_timing
        if is_cod:
            success = True
            description = "Cash on Delivery payment received"
        else:
            success = random.random() >= t["online_payment_failure_rate"]
            description = "Online payment" if success else "Online payment failed"

        return Transaction.new(
            order_id=order.order_id,
            transaction_type=TransactionType.PAYMENT.value,
            amount=order.order_total_amount,
            success=success,
            description=description,
        )

    def _create_refund_transaction(self, order: Order) -> Transaction:
        """Build a refund Transaction for a returned order."""
        return Transaction.new(
            order_id=order.order_id,
            transaction_type=TransactionType.REFUND.value,
            amount=order.order_total_amount,
            fake=self.fake,
            success=True,
            description="Refund issued for returned order",
        )

    async def _seed_lookup_tables(self):
        """
        Manually seed the core lookup tables (Order Status, Payment Status, Payment Method).
        These are seeded from constants.py or fixed lists to ensure consistency.
        """
        logging.info("Seeding lookup tables...")
        
        # 1. Order Statuses (from OrderStatus Enum)
        order_statuses = [{"order_status_id": status.value, "order_status_name": status.name} for status in OrderStatus]
        await asyncio.to_thread(self.writer.upsert, table="order_status", data=order_statuses, conflict_keys=["order_status_id"])

        # 2. Payment Statuses (from PaymentStatus Enum)
        payment_statuses = [{"payment_status_id": status.value, "payment_status_name": status.name} for status in PaymentStatus]
        await asyncio.to_thread(self.writer.upsert, table="payment_status", data=payment_statuses, conflict_keys=["payment_status_id"])

        # 3. Payment Methods (fixed list for user-friendly names)
        payment_methods = [{"payment_method_id": method.value, "payment_method_name": method.name} for method in PaymentMethod]
        await asyncio.to_thread(self.writer.upsert, table="payment_method", data=payment_methods, conflict_keys=["payment_method_id"])

    async def initialize(self):
        """
        One-time setup: creates all tables, then seeds reference and initial data.
        """
        try:
            logging.info("Setting up database schema...")
            await asyncio.to_thread(self.writer.create_tables_if_not_exists)
            await asyncio.to_thread(self.writer.truncate_all_tables)

            # ------------------------------------------------------------------
            # Phase 1: Independent Seeding (Parallel)
            # ------------------------------------------------------------------
            logging.info("Starting Parallel Phase 1 (Provinces, Brands, Lookups, Campaigns)...")
            
            # Prepare Provinces
            csv_path = os.path.join(os.path.dirname(__file__), "data", "vn_province_lookup.csv")
            provinces = []
            with open(csv_path, newline="", encoding="utf-8-sig") as f:
                for row in csv.DictReader(f):
                    provinces.append({
                        "province_name": row["province_ascii"],
                        "region_id":     int(row["region_id"]),
                        "latitude":      float(row["latitude"]),
                        "longitude":     float(row["longitude"]),
                    })
            
            # Prepare Campaigns and their Discounts (must be done together because campaign_type is not stored)
            init_campaigns = []
            init_discounts = []
            for _ in range(self.config.get("init_num_campaigns", 5)):
                campaign_type = Adscampaign.pick_type()
                campaign = Adscampaign.new(campaign_type=campaign_type, fake=self.fake)
                init_campaigns.append(campaign)
                for _ in range(random.randint(1, 3)):
                    s, e = Discount._window(campaign.started_at, campaign.expired_at, campaign_type=campaign_type)
                    init_discounts.append(Discount.new(
                        adscampaign_id=campaign.campaign_id,
                        discount_type=random.choice(["percent", "amount"]),
                        campaign_type=campaign_type,
                        started_at=s,
                        expired_at=e,
                        fake=self.fake
                    ))

            async def insert_campaigns_and_discounts():
                await asyncio.to_thread(self.writer.upsert, table="adscampaign", data=init_campaigns, conflict_keys=["campaign_id"])
                await asyncio.to_thread(self.writer.upsert, table="discount", data=init_discounts, conflict_keys=["discount_id"])

            # Run parallel inserts
            await asyncio.gather(
                self._seed_lookup_tables(),
                asyncio.to_thread(self.writer.upsert, table="province", data=provinces, conflict_keys=["province_name", "region_id"]),
                asyncio.to_thread(self.writer.upsert, table="brand", data=[Brand.new(brand_name=n, fake=self.fake) for n in sorted({name for names in BRANDS_BY_CATEGORY.values() for name in names})], conflict_keys=["brand_id"]),
                insert_campaigns_and_discounts(),
                asyncio.to_thread(self.writer.upsert, table="customer", data=[User.new(fake=self.fake) for _ in range(self.args.init_num_users)], conflict_keys=["customer_id"]),
            )

            # ------------------------------------------------------------------
            # Phase 2: Sequential Seeding with Dependencies
            # ------------------------------------------------------------------
            logging.info("Starting Phase 2 (Categories, Products, Addresses)...")
            
            # Get data for lookups
            brands = self.writer.select("brand")
            brand_map = {b["brand_name"]: b["brand_id"] for b in brands}
            customers = self.writer.select("customer")

            # Seed Categories
            parent_categories = [Category.new(category_name=cat, fake=self.fake) for cat in PRODUCT_CATEGORIES]
            await asyncio.to_thread(self.writer.upsert, table="category", data=parent_categories, conflict_keys=["category_id"])
            parent_map = {c.category_name: c.category_id for c in parent_categories}

            child_categories = []
            child_parent_map = {}
            for parent_name, subcategories in PRODUCT_CATEGORIES.items():
                pid = parent_map[parent_name]
                for sub in subcategories:
                    cat = Category.new(category_name=sub, fake=self.fake, parent_category_id=pid)
                    child_categories.append(cat)
                    child_parent_map[sub] = cat.category_id
            await asyncio.to_thread(self.writer.upsert, table="category", data=child_categories, conflict_keys=["category_id"])

            # Seed Products
            products = []
            for parent_name, subcategories in PRODUCT_CATEGORIES.items():
                cat_brands = BRANDS_BY_CATEGORY.get(parent_name, [])
                for sub in subcategories:
                    cid = child_parent_map[sub]
                    for _ in range(random.randint(3, 8)):
                        bname = random.choice(cat_brands) if cat_brands else "Generic"
                        bid = brand_map.get(bname, self.fake.uuid4())
                        products.append(Product.new(category_id=cid, brand_id=bid, fake=self.fake, category_name=parent_name, subcategory=sub, brand_name=bname))
            
            # Final Parallel Phase
            await asyncio.gather(
                asyncio.to_thread(self.writer.upsert, table="product", data=products, conflict_keys=["product_id"]),
                asyncio.to_thread(self.writer.upsert, table="address", data=[Address.new(customer_id=c["customer_id"], province_id=random.randint(1, len(provinces)), fake=self.fake) for c in customers for _ in range(random.randint(1, 2))], conflict_keys=["address_id"]),
            )
            
            logging.info(f" Initialized {len(init_campaigns)} campaigns, {len(init_discounts)} discounts, {len(products)} products")
            logging.info(f" Initialized {len(init_campaigns)} campaigns, {len(init_discounts)} discounts")

            logging.info("Initialization complete.")
            return True

        except (SQLAlchemyError, OperationalError) as e:
            logging.critical(
                f"A fatal error occurred during initial setup. Cannot continue. Error: {e}"
            )
            return False

    async def _simulate_purchases(self):
        """Generates a complete user session using a Markov Chain navigation graph."""
        from data.catalog import NAVIGATION_GRAPH, EVENT_TIMING as t, EVENT_TRAFFIC_SOURCES, BROWSER_DISTRIBUTION
        
        topic = "ecommerce.events.clickstream"
        session_id = str(uuid.uuid4())
        
        # 1. Decide if user is logged in
        customer_id = None
        address_id = None
        if random.random() < LOGGED_IN_PROB:  # 70% chance to be logged in
            customer_list = await asyncio.to_thread(
                self.writer.select, table="customer", order_by="RANDOM()", limit=5
            )
            for c in customer_list:
                if c["customer_id"] not in self.busy_customers:
                    customer_id = c["customer_id"]
                    self.busy_customers.add(customer_id)
                    break

            if customer_id:
                # Get address
                address_list = await asyncio.to_thread(
                    self.writer.select,
                        table="address",
                        where_clause="customer_id = :cid",
                        where_params={"cid": customer_id},
                        order_by="RANDOM()",
                        limit=1,
                    )
                if address_list:
                    address_id = address_list[0]["address_id"]

        try:
                # 2. Session Context
            from data.catalog import UTM_SOURCE_TO_MEDIUM, DEVICE_DISTRIBUTION
            utm_source = random.choices(list(EVENT_TRAFFIC_SOURCES.keys()), weights=list(EVENT_TRAFFIC_SOURCES.values()))[0]
            utm_medium = UTM_SOURCE_TO_MEDIUM.get(utm_source, "organic")
        
            utm_campaign = "none"
            if random.random() > 0.5:
                now = datetime.datetime.now()
                active_campaigns = await asyncio.to_thread(
                    self.writer.select,
                    table="adscampaign",
                    where_clause="started_at <= :now AND expired_at >= :now",
                    where_params={"now": now},
                    order_by="RANDOM()",
                    limit=1,
                )
                if active_campaigns:
                    utm_campaign = active_campaigns[0].get("campaign_title", "none")
                
            browser = random.choices(list(BROWSER_DISTRIBUTION.keys()), weights=list(BROWSER_DISTRIBUTION.values()))[0]
            device = random.choices(list(DEVICE_DISTRIBUTION.keys()), weights=list(DEVICE_DISTRIBUTION.values()))[0]
            ip_addr = self.fake.ipv4()

            def emit(event_type: str, properties: dict, timestamp=None):
                if timestamp is None:
                    timestamp = datetime.datetime.now()
                self.event_producer.send_event(topic, {
                    "event_id": str(uuid.uuid4()),
                    "event_type": event_type,
                    "session_id": session_id,
                    "customer_id": customer_id,
                    "timestamp": timestamp.isoformat(),
                    "device_type": device,
                    "browser": browser,
                    "ip_address": ip_addr,
                    "utm": {
                        "source": utm_source,
                        "medium": utm_medium,
                        "campaign": utm_campaign
                    },
                    "properties": properties
                })

            # 3. State Machine Initialization
            current_state = "home"
            cart = []
            current_product = None  # Holds the dict of the product currently being viewed
        
            # Helper to fetch a random product
            async def fetch_random_product():
                prods = await asyncio.to_thread(self.writer.select, table="product", order_by="RANDOM()", limit=1)
                return prods[0] if prods else None

            while current_state not in ["exit", "purchase"]:
                # Evaluate conditional transitions
                raw_transitions = NAVIGATION_GRAPH.get(current_state, {"exit": 1.0})
                # valid_transitions = {}
                # for next_action, prob in raw_transitions.items():
                #     if next_action == "remove_from_cart" and not cart: continue
                #     if next_action == "purchase" and not cart: continue
                #     valid_transitions[next_action] = prob
                
                # total_prob = sum(valid_transitions.values())
                # if total_prob == 0:
                #     valid_transitions = {"exit": 1.0}
                # else:
                #     for k in valid_transitions: valid_transitions[k] /= total_prob
                    
                next_action = random.choices(list(raw_transitions.keys()), weights=list(raw_transitions.values()))[0]

                # Execute Action
                if next_action in ["home", "category", "cart", "checkout"]:
                    current_state = next_action
                    emit("page_view", {"page_url": f"/{current_state}", "page_title": current_state.title()})
                    # Using general page view delay
                    await asyncio.sleep(random.uniform(t["page_view_delay_min"], t["page_view_delay_max"]))
                
                elif next_action == "item":
                    current_state = "item"
                    current_product = await fetch_random_product()
                    if current_product:
                        emit("page_view", {"page_url": f"/item/{current_product['product_id']}", "page_title": "Product Details"})
                        # emit("product_viewed", {"product_id": current_product["product_id"], "price": float(current_product.get("product_price", 0))})
                    await asyncio.sleep(random.uniform(t["product_view_delay_min"], t["product_view_delay_max"]))
                
                elif next_action == "add_to_cart":
                    # Stays in 'item' state
                    if current_product:
                        qty = random.randint(1, 3)
                        price = float(current_product.get("product_price", 0))
                        tax = float(current_product.get("product_tax", 0))
                    
                        # Check if already in cart
                        existing = next((i for i in cart if i["product_id"] == current_product["product_id"]), None)
                        if existing:
                            existing["quantity"] += qty
                        else:
                            cart.append({"product_id": current_product["product_id"], "price": price, "tax": tax, "quantity": qty})
                        
                        emit("add_to_cart", {"product_id": current_product["product_id"], "quantity": qty, "cart_total": price * qty})
                    await asyncio.sleep(random.uniform(t["add_to_cart_delay_min"], t["add_to_cart_delay_max"]))
                
                elif next_action == "remove_from_cart":
                    # Stays in 'cart' state
                    if cart:
                        removed_item = cart.pop(random.randint(0, len(cart) - 1))
                        emit("remove_from_cart", {"product_id": removed_item["product_id"], "quantity": removed_item["quantity"]})
                    await asyncio.sleep(random.uniform(1, 3))
                
                elif next_action == "purchase":
                    # Only proceeds if logged in, has address, and cart not empty
                    if not customer_id or not address_id or not cart:
                        current_state = "exit"
                        continue
                
                    # Fetch discounts
                    available_discounts = None
                    if random.random() < DISCOUNT_APPLY_PROB:
                        try:
                            now = datetime.datetime.now()
                            d_records = await asyncio.to_thread(self.writer.select, table="discount", where_clause="started_at <= :now AND expired_at >= :now", where_params={"now": now})
                            if d_records:
                                from data.models import Discount
                                available_discounts = [Discount.from_dict(r) for r in d_records]
                        except Exception:
                            pass
                
                    payment_method = random.choice(list(PaymentMethod))
                    order = Order.new(customer_id=customer_id, address_id=address_id, order_items_data=cart, fake=self.fake, available_discounts=available_discounts, payment_method_id=payment_method.value)
                
                    order_items = [
                        OrderItem.new(order_id=order.order_id, product_id=c["product_id"], product_price=c["price"], product_tax=c["tax"], quantity=c["quantity"], fake=self.fake)
                        for c in cart
                    ]
                
                    order_status_history = OrderStatusHistory.new(order_id=order.order_id, order_status_id=OrderStatus.PROCESSING.value, comments="Order placed", fake=self.fake)

                    # Execute concurrent write and emit
                    async def emit_purchase_async():
                        emit("purchase", {"order_id": order.order_id, "total_amount": order.order_total_amount, "item_count": len(cart)}, timestamp=datetime.datetime.now())

                    await asyncio.gather(
                        asyncio.to_thread(self.writer.upsert, table="order", data=[order], conflict_keys=["order_id"]),
                        emit_purchase_async()
                    )
                
                    await asyncio.gather(
                        asyncio.to_thread(self.writer.upsert, table="order_detail", data=order_items, conflict_keys=["order_detail_id"]),
                        asyncio.to_thread(self.writer.upsert, table="order_status_history", data=[order_status_history], conflict_keys=["order_id", "order_status_id"])
                    )
                
                    # Online payment check
                    is_cod = order.payment_method_id == PaymentMethod.CASH_ON_DELIVERY.value
                
                    if not is_cod:
                        t = self._txn_timing
                        delay = random.randint(
                            t["online_payment_delay_min_s"],
                            t["online_payment_delay_max_s"],
                        )
                        await asyncio.sleep(delay)
                        txn = self._create_payment_transaction(order, is_cod=False)
                        await asyncio.to_thread(
                            self.writer.upsert,
                            table="transaction",
                            data=[txn],
                            conflict_keys=["transaction_id"],
                        )
                        # Update payment_status on the order based on transaction outcome
                        new_payment_status = (
                            PaymentStatus.COMPLETED.value if txn.transaction_status
                            else PaymentStatus.FAILED.value
                        )
                        order.payment_status_id = new_payment_status
                        await asyncio.to_thread(
                            self.writer.upsert, table="order", data=[order], conflict_keys=["order_id"], update_fields=["payment_status_id"]
                        )
                        logging.info(
                            f"Payment transaction for order {order.order_id}: "
                            f"{'SUCCESS' if txn.transaction_status else 'FAILED'}"
                        )
        
                    logging.info(f"Created order {order.order_id} with {len(cart)} items")
                    # Push the order to the queue so the background worker can advance its lifecycle
                    self.active_orders_queue.put_nowait(order)
                    current_state = "purchase"
                
                elif next_action == "exit":
                    current_state = "exit"
        
        finally:
            if customer_id and customer_id in self.busy_customers:
                self.busy_customers.discard(customer_id)



    async def _order_queue_worker(self):
        """Continuously pulls orders from the active queue and spawns state transitions."""
        try:
            while True:
                random_order = await self.active_orders_queue.get()
                asyncio.create_task(self._simulate_order_update(random_order))
        except asyncio.CancelledError:
            logging.info("Order queue worker shutting down.")

    async def _simulate_order_update(self, random_order: Order):
        """Logic for updating an order with status tracking using an internal queue."""
        # Implement state machine for order status and shipping status transitions
        # PENDING -> IN_TRANSIT (SHIPPED) -> DELIVERED (DELIVERED) -> [RETURNED]
        try:
            t = self._txn_timing
            # State machine for order status and shipping status transitions
            # PENDING -> IN_TRANSIT (SHIPPED) -> DELIVERED (DELIVERED) -> [RETURNED]
            if random_order.shipping_status == ShippingStatus.PENDING.value:
                # Add delay before shipping or cancellation
                delay = random.randint(t["order_shipped_delay_min_s"], t["order_shipped_delay_max_s"])
                await asyncio.sleep(delay)
                
                # Check if the customer decides to cancel before it ships
                if random.random() < ORDER_CANCEL_RATE:
                    random_order.order_status_id = OrderStatus.CANCELLED.value
                    new_order_status = OrderStatus.CANCELLED
                    status_comment = "Customer canceled the order before shipping"
                    
                    # If they paid online, issue a refund transaction
                    if random_order.payment_method_id != PaymentMethod.CASH_ON_DELIVERY.value and random_order.payment_status_id == PaymentStatus.COMPLETED.value:
                        delay = random.randint(t["refund_delay_min_s"], t["refund_delay_max_s"])
                        await asyncio.sleep(delay)
                        txn = self._create_refund_transaction(random_order)
                        await asyncio.to_thread(
                            self.writer.upsert, table="transaction", data=[txn], conflict_keys=["transaction_id"]
                        )
                        random_order.payment_status_id = PaymentStatus.REFUNDED.value
                        logging.info(f"Refund transaction created for canceled order {random_order.order_id}")
                else:
                    random_order.shipping_status = ShippingStatus.IN_TRANSIT.value
                    random_order.shipped_at = datetime.datetime.now()
                    new_order_status = OrderStatus.SHIPPED
                    status_comment = "Order picked and handed to carrier"

            elif random_order.shipping_status == ShippingStatus.IN_TRANSIT.value:
                # Add delay before delivery
                delay = random.randint(t["order_delivered_delay_min_s"], t["order_delivered_delay_max_s"])
                await asyncio.sleep(delay)
                
                # Transition from In Transit to Delivered
                random_order.shipping_status = ShippingStatus.DELIVERED.value
                new_order_status = OrderStatus.DELIVERED
                status_comment = "Package delivered to destination"

                # COD payment: create payment transaction on delivery.
                if random_order.payment_method_id == PaymentMethod.CASH_ON_DELIVERY.value:
                    t = self._txn_timing
                    delay = random.randint(
                        t["cod_payment_delay_min_s"],
                        t["cod_payment_delay_max_s"],
                    )
                    await asyncio.sleep(delay)
                    txn = self._create_payment_transaction(random_order, is_cod=True)
                    await asyncio.to_thread(
                        self.writer.upsert,
                        table="transaction", data=[txn], conflict_keys=["transaction_id"]
                    )
                    random_order.payment_status_id = PaymentStatus.COMPLETED.value
                    logging.info(f"COD payment transaction created for order {random_order.order_id}")

            elif random_order.shipping_status == ShippingStatus.DELIVERED.value:
                # Potential return transition
                if random.random() < ORDER_RETURN_RATE: # 10% return rate for testing
                    random_order.shipping_status = ShippingStatus.RETURNED.value
                    random_order.returned_at = datetime.datetime.now()
                    new_order_status = OrderStatus.RETURNED
                    status_comment = "Customer initiated return"

                    # Refund transaction on return
                    t = self._txn_timing
                    delay = random.randint(
                        t["refund_delay_min_s"],
                        t["refund_delay_max_s"],
                    )
                    await asyncio.sleep(delay)
                    txn = self._create_refund_transaction(random_order)
                    await asyncio.to_thread(
                        self.writer.upsert,
                        table="transaction", data=[txn], conflict_keys=["transaction_id"]
                    )
                    random_order.payment_status_id = PaymentStatus.REFUNDED.value
                    logging.info(f"Refund transaction created for order {random_order.order_id}")
                else:
                    # Terminal state reached (Delivered, no return)
                    return
            else:
                return # Already in a terminal state

            # Re-queue the order so it can potentially advance to the next state later
            if new_order_status and new_order_status.value not in (OrderStatus.RETURNED.value, OrderStatus.CANCELLED.value):
                self.active_orders_queue.put_nowait(random_order)

            if new_order_status:
                new_status_id = new_order_status.value
                random_order.order_status_id = new_status_id # Sync order_status with shipping_status
                
                order_status_history = OrderStatusHistory.new(
                    order_id=random_order.order_id,
                    order_status_id=new_status_id,
                    comments=status_comment,
                    fake=self.fake,
                )
                
                random_order.updated_at = datetime.datetime.now()
                
                # Atomic-like write of order and history
                try:
                    await asyncio.gather(
                        asyncio.to_thread(self.writer.upsert, table="order", data=[random_order], conflict_keys=["order_id"]),
                        asyncio.to_thread(
                            self.writer.upsert,
                            table="order_status_history",
                            data=[order_status_history],
                            conflict_keys=["order_id", "order_status_id"]
                        )
                    )
                    logging.info(f"Updated order {random_order.order_id}: Shipping={ShippingStatus(random_order.shipping_status).name}, Status={new_order_status.name}")
                except Exception as e:
                    if "unique constraint" in str(e).lower():
                        logging.debug(f"Concurrency collision for order {random_order.order_id}: {e}")
                    else:
                        raise
        except Exception as e:
            logging.error(f"Failed to update order {random_order.order_id}: {e}")

    async def _simulate_side_tasks(self):
        """Runs secondary simulation events based on their respective probabilities."""
        side_tasks = []

        if (
            self.args.user_create_prob > 0
            and random.random() < self.args.user_create_prob
        ):
            logging.info("Creating a new user...")
            new_user = User.new(fake=self.fake)
            new_address = Address.new(
                customer_id=new_user.customer_id,
                province_id=random.randint(1, 63), # Assuming 63 provinces as per initialize
                fake=self.fake
            )
            async def insert_new_user():
                await asyncio.to_thread(self.writer.upsert, table="customer", data=[new_user], conflict_keys=["customer_id"])
                await asyncio.to_thread(self.writer.upsert, table="address", data=[new_address], conflict_keys=["address_id"])
                
            side_tasks.append(insert_new_user())

        if (
            self.args.user_update_prob > 0
            and random.random() < self.args.user_update_prob
        ):
            customer_list = await asyncio.to_thread(
                self.writer.select, table="customer", order_by="RANDOM()", limit=5
            )
            customer_to_update = None
            for c in customer_list:
                if c["customer_id"] not in self.busy_customers:
                    customer_to_update = User.from_dict(c)
                    break
            
            if customer_to_update:
                async def update_user_profile():
                    self.busy_customers.add(customer_to_update.customer_id)
                    try:
                        # Sleep at least 1 second to simulate time taken to perform the action
                        await asyncio.sleep(random.uniform(1, 3))
                        customer_to_update.customer_mobile = self.fake.phone_number()
                        customer_to_update.updated_at = datetime.datetime.now()
                        await asyncio.to_thread(
                            self.writer.upsert,
                            table="customer",
                            data=[customer_to_update],
                            conflict_keys=["customer_id"],
                            update_fields=["customer_mobile", "updated_at"]
                        )
                        logging.info(f"Updated user profile for {customer_to_update.customer_id}")
                    finally:
                        self.busy_customers.discard(customer_to_update.customer_id)
                
                side_tasks.append(update_user_profile())

        if side_tasks:
            await asyncio.gather(*side_tasks)

    async def _simulate_promotions(self):
        """
        Background task: periodically creates ad campaigns and their discounts.

        Campaign type, duration, and discount scheduling are all driven by the
        "promotions" key in the config file. See catalog.py for default values
        and the config schema.
        """
        logging.info("Promotion simulation background task started.")

        while True:
            try:
                # Wait 2-10 minutes between campaigns (real-world cadence is slower,
                # but we want frequent data in simulation mode).
                await asyncio.sleep(random.uniform(120, 600))

                # Step 1: pick the campaign type — drives duration, scheduling,
                # and discount archetypes, but is NOT stored on the DB record.
                campaign_type = Adscampaign.pick_type()

                # Step 2: create the campaign with type-driven duration/scheduling.
                campaign = Adscampaign.new(
                    campaign_type=campaign_type,
                    fake=self.fake,
                )

                await asyncio.to_thread(
                    self.writer.upsert,
                    table="adscampaign",
                    data=[campaign],
                    conflict_keys=["campaign_id"],
                )

                # Each discount independently picks an archetype (flash/full/partial)
                # conditioned on the campaign type, and flash discounts are
                # scheduled to peak shopping hours.
                discounts = []
                for _ in range(random.randint(1, 5)):
                    discount_type = random.choice(["percent", "amount"])
                    started_at, expired_at = Discount._window(
                        campaign.started_at,
                        campaign.expired_at,
                        campaign_type=campaign.campaign_type,
                    )
                    discounts.append(
                        Discount.new(
                            adscampaign_id=campaign.campaign_id,
                            discount_type=discount_type,
                            started_at=started_at,
                            expired_at=expired_at,
                            fake=self.fake,
                        )
                    )

                if discounts:
                    await asyncio.to_thread(
                        self.writer.upsert,
                        table="discount",
                        data=discounts,
                        conflict_keys=["discount_id"],
                    )

                logging.info(
                    f"Marketing: [{campaign_type.upper()}] "
                    f"'{campaign.campaign_title}' "
                    f"active until {campaign.expired_at:%Y-%m-%d %H:%M}. "
                    f"Created {len(discounts)} discount(s)."
                )

            except asyncio.CancelledError:
                logging.info("Promotion simulation task shutting down.")
                break
            except Exception as e:
                logging.error(f"Error in promotion simulation: {e}")
                await asyncio.sleep(30)

    async def run(self):
        """The main simulation loop, which orchestrates primary and side tasks."""
        # Start background tasks
        promotion_task = asyncio.create_task(self._simulate_promotions())
        order_worker_task = asyncio.create_task(self._order_queue_worker())
        
        active_tasks = set()
        current_iteration = 0
        try:
            while True:
                if 0 < self.args.max_iter <= current_iteration:
                    logging.info(
                        f"Stopping data generation after reaching {current_iteration} iterations."
                    )
                    break

                try:
                    wait_time = random.expovariate(self.args.avg_qps)
                    await asyncio.sleep(wait_time)

                    # Spawn tasks instead of awaiting them to allow higher QPS
                    purchase_task = asyncio.create_task(self._simulate_purchases())
                    active_tasks.add(purchase_task)
                    purchase_task.add_done_callback(active_tasks.discard)

                    side_task = asyncio.create_task(self._simulate_side_tasks())
                    active_tasks.add(side_task)
                    side_task.add_done_callback(active_tasks.discard)
                except (SQLAlchemyError, OperationalError) as e:
                    self.consecutive_db_errors += 1
                    logging.warning(
                        f"A database error occurred. Consecutive error count: "
                        f"{self.consecutive_db_errors}/{self.max_consecutive_errors}. "
                        f"Error: {e}"
                    )
                    if self.consecutive_db_errors >= self.max_consecutive_errors:
                        logging.critical(
                            f"Exceeded max consecutive DB error limit "
                            f"({self.max_consecutive_errors}). "
                            "The simulation will now stop."
                        )
                        break
                    await asyncio.sleep(5)
                else:
                    if self.consecutive_db_errors > 0:
                        logging.info(
                            "Database operation successful. Resetting consecutive error counter."
                        )
                    self.consecutive_db_errors = 0
                    current_iteration += 1
        finally:
            # Gracefully cancel background tasks
            logging.info("Simulation loop has finished.")
            if active_tasks:
                logging.info(f"Waiting for {len(active_tasks)} background tasks to finish...")
                await asyncio.gather(*active_tasks, return_exceptions=True)
            
            promotion_task.cancel()
            order_worker_task.cancel()
            await asyncio.gather(promotion_task, order_worker_task, return_exceptions=True)
            
            logging.info("Closing database connection...")
            self.writer.close()
            logging.info("Database connection closed.")

    async def close(self):
        """Gracefully closes the database connection."""
        if self.writer:
            self.writer.close()
        logging.info("Database connection closed.")


async def run_simulation(args: argparse.Namespace, config: dict = None):
    """Orchestrates the creation, initialization, and execution of the simulator."""
    simulator = ECommSimulator(args, Faker(), config)
    try:
        if await simulator.initialize():
            await simulator.run()
    except asyncio.CancelledError:
        logging.warning("Data generation task was cancelled.")
    finally:
        await simulator.close()


def main():
    """
    Parses command-line arguments and starts the data generation simulation.
    """
    # fmt: off
    parser = argparse.ArgumentParser(description="Generate  eCommerce data")
    ## --- General Arguments ---
    parser.add_argument("--avg-qps", type=float, default=200.0, help="Average events per second.")
    parser.add_argument("--max-iter", type=int, default=-1, help="Max number of successful iterations. Default -1 for infinite.")
    ## --- User Arguments ---
    parser.add_argument("--init-num-users", type=int, default=200, help="Initial number of users to create.")
    parser.add_argument("--user-create-prob", type=float, default=0.05, help="Probability of generating a new user. Default is 0.05. Set to 0 to disable.")
    parser.add_argument("--user-update-prob", type=float, default=0.1, help="Probability of updating a user address. Default is 0.1. Set to 0 to disable.")
    ## --- Order Arguments ---
    parser.add_argument("--order-update-prob", type=float, default=0.4, help="Probability of updating an order status. Default is 0.4. Set to 0 to disable.")
    ## --- Ghost Event Arguments ---
    parser.add_argument("--ghost-create-prob", type=float, default=0.2, help="Probability of generating a ghost event. Default is 0.2. Set to 0 to disable.")
    ## --- Database Arguments ---
    parser.add_argument("--db-host", default=os.getenv("POSTGRES_HOST", "localhost:5433"), help="Database host.")
    parser.add_argument("--db-user", default=os.getenv("POSTGRES_USER", "postgres"), help="Database user.")
    parser.add_argument("--db-password", default=os.getenv("POSTGRES_PASSWORD", "postgres_password"), help="Database password.")
    parser.add_argument("--db-name", default=os.getenv("POSTGRES_DB", "default_db"), help="Database name.")
    parser.add_argument("--db-schema", default="demo", help="Database schema.")
    parser.add_argument("--db-batch-size", type=int, default=2000, help="Batch size for database writes.")
    ## --- Kafka Arguments ---
    parser.add_argument("--bootstrap-servers", type=str, default="localhost:9092", help="Bootstrap server addresses.")
    parser.add_argument("--topic-prefix", type=str, default="ecomm", help="Kafka topic prefix.")
    parser.add_argument("--config", type=str, help="Path to YAML config file.")
    # fmt: on

    load_dotenv()
    args = parser.parse_args()
    logging.info(args)
    
    config_data = {}
    if args.config:
        import yaml
        try:
            with open(args.config, "r") as f:
                config_data = yaml.safe_load(f)
        except Exception as e:
            logging.error(f"Failed to load config file: {e}")

    try:
        asyncio.run(run_simulation(args, config_data))
    except KeyboardInterrupt:
        logging.warning("Data generator stopped by user.")
    except Exception as e:
        logging.critical(f"An unexpected top-level error occurred: {e}", exc_info=True)
    finally:
        logging.info("Application shutdown complete.")


if __name__ == "__main__":
    main()
