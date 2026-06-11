from data.constants import PaymentStatus
from data.constants import OrderStatus
from data.catalog import TRAFFIC_SOURCES
from data.catalog import AGE_DISTRIBUTION
from data.catalog import (
    MAX_AMOUNT_DISCOUNT,
    MIN_AMOUNT_DISCOUNT,
    MAX_PERCENT_DISCOUNT,
    MIN_PERCENT_DISCOUNT,
    CAMPAIGN_SUFFIXES,
    CAMPAIGN_TYPES,
    DISCOUNT_ARCHETYPES_BY_CAMPAIGN,
    PEAK_HOURS,
    FLASH_DISCOUNT_HOUR_WEIGHTS,
)
import hashlib
import datetime
import dataclasses
import random
import logging
import inspect
import uuid
from collections import OrderedDict
from decimal import Decimal
from typing import List, Optional
from typing_extensions import Self

from faker import Faker

from data.constants import ShippingStatus
from data.catalog import (
    AGE_DISTRIBUTION,
    TRAFFIC_SOURCES,
    ADDRESS_TYPE_DISTRIBUTION,
    PRODUCT_COST_RANGE,
    SHIPPING_METHOD_DISTRIBUTION,
    PRODUCT_TEMPLATES,
    PRODUCT_SPECS,
    CAMPAIGN_SUFFIXES,
)

logging.basicConfig(
    level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s"
)


def get_additional_ddls(schema: str):
    return {
        "heartbeat": inspect.cleandoc(f"""
            CREATE TABLE IF NOT EXISTS {schema}.heartbeat (
                id INT PRIMARY KEY,
                ts TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW()
            );
        """),
        "province": inspect.cleandoc(f"""
            CREATE TABLE IF NOT EXISTS {schema}.province (
                province_id   SERIAL PRIMARY KEY,
                province_name VARCHAR(100) NOT NULL,
                region_id     INTEGER  NOT NULL,
                latitude      NUMERIC,
                longitude     NUMERIC,
                created_at    TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL DEFAULT (now() AT TIME ZONE 'Asia/Ho_Chi_Minh'),
                UNIQUE(province_name, region_id)
            );
        """)

    }





class ModelMixin:
    @classmethod
    def from_dict(cls, data: dict):
        valid_keys = {f.name for f in dataclasses.fields(cls)} # type: ignore
        filtered = {k: v for k, v in data.items() if k in valid_keys}
        return cls(**filtered)

    @classmethod
    def from_rows(cls, rows: List[dict]):
        return [cls.from_dict(row) for row in rows]


@dataclasses.dataclass
class User(ModelMixin):
    customer_id: str
    customer_first_name: str
    customer_last_name: str
    customer_email: str
    customer_dob: datetime.date
    customer_gender: int # M=1, F=0
    customer_password: str
    customer_mobile: str
    customer_traffic_source: str

    @classmethod
    def new(
        cls,
        *,
        fake: Faker,
    ) -> Self:
        if fake is None:
            fake = Faker()
        
        gender = fake.random_element(elements=(0, 1))
        first_name = (
            fake.first_name_male() if gender == 1 else fake.first_name_female()
        )
        last_name = fake.last_name_nonbinary()

        
        age_ranges = [tuple(map(int, k.split('-'))) for k in AGE_DISTRIBUTION.keys()]
        probabilities = list(AGE_DISTRIBUTION.values())
        chosen_range = fake.random_choices(
            elements=OrderedDict(zip(age_ranges, probabilities)),
            length=1,
        )[0]
        dob = fake.date_of_birth(minimum_age=chosen_range[0], maximum_age=chosen_range[1])
        password = hashlib.sha256(fake.password().encode('utf-8')).hexdigest()
        
        traffic_sources = list(TRAFFIC_SOURCES.keys())
        traffic_probs = list(TRAFFIC_SOURCES.values())
            
        traffic_source = fake.random_choices(
            elements=OrderedDict(zip(traffic_sources, traffic_probs)),
            length=1,
        )[0]
        return cls(
            customer_id=str(uuid.uuid4()),
            customer_first_name=first_name,
            customer_last_name=last_name,
            customer_email=f"{first_name.lower()}.{last_name.lower()}{dob.strftime('%Y%m')}@{fake.safe_domain_name()}",
            customer_dob=dob,
            customer_gender=gender,
            customer_password=password,
            customer_mobile=fake.phone_number(),
            customer_traffic_source=traffic_source,
        )

    def update(
        self,
        updated_data: dict,
    ) -> Self:
        BLACKLIST = {"id", "traffic_source", "dob", "gender", "created_at", "updated_at"}

        filtered_data = {}
        for key, value in updated_data.items():
            if key in BLACKLIST:
                logging.warning(f"Cannot update restricted field: '{key}'. Skipping.")
                continue
            filtered_data[key] = value
                
        current_data = dataclasses.asdict(self)
        
        filtered_data["updated_at"] = datetime.datetime.now()
            
        merged_data = {**current_data, **filtered_data}
        return self.from_dict(merged_data)

    def __str__(self):
        return f"User(id={self.id}, name='{self.first_name} {self.last_name}', source='{self.traffic_source}')"

    @staticmethod
    def ddl(schema: str = "public"):
        return inspect.cleandoc(f"""
        CREATE TABLE IF NOT EXISTS {schema}.customer
            (
                customer_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                customer_first_name VARCHAR(100) NOT NULL,
                customer_last_name VARCHAR(100) NOT NULL,
                customer_dob DATE,
                customer_gender INT, -- 1: Male, 0: Female
                customer_traffic_source VARCHAR(255), 
                customer_password VARCHAR(255),
                customer_email VARCHAR(255) NOT NULL UNIQUE,
                customer_mobile VARCHAR(255) NOT NULL UNIQUE,
                created_at TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL DEFAULT (now() AT TIME ZONE 'Asia/Ho_Chi_Minh'),
                updated_at TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL DEFAULT (now() AT TIME ZONE 'Asia/Ho_Chi_Minh'),

                CONSTRAINT customer_gender_check CHECK (customer_gender IN (1, 0))
            );
        """)


@dataclasses.dataclass
class Adscampaign(ModelMixin):
    """
    Advertising campaign that may have associated discounts.

    campaign_type is used by the simulation to derive duration and discount
    archetypes, but is NOT stored in the database — use Adscampaign.pick_type()
    before calling new() if you need the type for discount scheduling.
    """
    campaign_id: str   # Generated in Python so discounts can reference it immediately
    campaign_title: str
    started_at: datetime.datetime
    expired_at: datetime.datetime

    @staticmethod
    def pick_type() -> str:
        """
        Choose a campaign type from the weighted distribution.

        Separated from new() so the caller can capture the chosen type and
        pass it to Discount._window() for archetype-aware scheduling,
        without needing to store the type on the campaign record itself.

        Config key: "campaign_types" to override weights and duration bands.
        """
        types   = list(CAMPAIGN_TYPES.keys())
        weights = [CAMPAIGN_TYPES[t]["weight"] for t in types]
        return random.choices(types, weights=weights, k=1)[0]

    @classmethod
    def new(
        cls,
        *,
        campaign_type: str,
        fake: Faker,
        started_at: Optional[datetime.datetime] = datetime.datetime.now(),
    ) -> Self:
        """
        Create a new advertising campaign.

        campaign_type controls:
        - Duration: each type has a realistic min/max hour band.
        - Scheduling: weekend campaigns are aligned to the next Friday at 18:00.
        It is NOT stored on the record; the caller must track it separately
        if needed for discount scheduling.

        Config key: "campaign_types" to override the duration bands.
        """
        band    = CAMPAIGN_TYPES[campaign_type]
        duration_hours = random.randint(band["min_hours"], band["max_hours"])

        # Weekend campaigns are scheduled to begin on the next Friday at 18:00
        # so their window naturally covers Fri–Sun shopping behaviour.
        if campaign_type == "weekend":
            days_to_friday = (4 - started_at.weekday()) % 7
            if days_to_friday == 0 and started_at.hour >= 18:
                days_to_friday = 7  # Already past Friday evening — use next week
            started_at = (
                started_at + datetime.timedelta(days=days_to_friday)
            ).replace(hour=18, minute=0, second=0, microsecond=0)

        return cls(
            campaign_id=str(uuid.uuid4()),
            campaign_title=f"{fake.word().capitalize()} {campaign_type.capitalize()} {random.choice(CAMPAIGN_SUFFIXES)}",
            started_at=started_at,
            expired_at=started_at + datetime.timedelta(hours=duration_hours),
        )

    def is_active(self) -> bool:
        """Check if campaign is currently active."""
        now = datetime.datetime.now()
        return self.started_at <= now <= self.expired_at

    def __str__(self):
        return f"Adscampaign(id={self.campaign_id}, title='{self.campaign_title}')"

    @staticmethod
    def ddl(schema: str = "public"):
        return inspect.cleandoc(f"""
        CREATE TABLE IF NOT EXISTS {schema}.adscampaign
        (
            campaign_id    UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
            campaign_title VARCHAR(255),
            started_at     TIMESTAMP WITHOUT TIME ZONE NOT NULL,
            expired_at     TIMESTAMP WITHOUT TIME ZONE NOT NULL,
            created_at     TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL DEFAULT (now() AT TIME ZONE 'Asia/Ho_Chi_Minh')
        );
        """)



@dataclasses.dataclass
class Discount(ModelMixin):
    """Discount associated with an advertising campaign.
    
    Discounts can be either percentage-based or fixed amount.
    They have validity periods and must be linked to an active campaign.
    """
    discount_id: str
    adscampaign_id: Optional[str]
    discount_type: str  # 'percent' or 'amount'
    discount_value: float
    discount_code: Optional[str]
    started_at: datetime.datetime
    expired_at: datetime.datetime

    @staticmethod
    def _snap_to_peak_hour(
        dt: datetime.datetime,
        peak_hours: list,
        hour_weights: list,
    ) -> datetime.datetime:
        """
        Snap a datetime to a weighted peak shopping hour on the same or next day.

        If the given time is already past all peak hours for that day, the
        chosen peak is moved to the following day.
        """
        target_hour = random.choices(peak_hours, weights=hour_weights, k=1)[0]
        if dt.hour >= max(peak_hours):
            dt = dt + datetime.timedelta(days=1)
        return dt.replace(hour=target_hour, minute=0, second=0, microsecond=0)

    @staticmethod
    def _window(
        campaign_started_at: datetime.datetime,
        campaign_expired_at: datetime.datetime,
        campaign_type: str = "weekly",
    ) -> tuple:
        """
        Derive a realistic discount validity window inside a campaign.

        The archetype probabilities are driven by the campaign type so that
        the discount mix reflects real marketing behaviour:

          flash    → mostly flash burst coupons (high %, short window)
          weekend  → mostly full-campaign codes with some flash bursts
          weekly   → balanced mix of full and partial/staged rollouts
          seasonal → mostly partial (early-bird, last-chance waves)
          loyalty  → almost entirely partial/staged, very few flash

        Flash discounts are additionally scheduled to start at a realistic
        peak-traffic hour (configurable) rather than an arbitrary timestamp.

        Config keys (all optional, fall back to catalog defaults):
          promotions.discount_archetypes  — archetype weights per campaign type
          promotions.flash_peak_hours     — peak hours list by weekday/weekend
          promotions.flash_hour_weights   — probability weights per peak hour

        Returns:
            (started_at, expired_at) tuple, both within the campaign window.
        """

        # Archetype probabilities for this campaign type
        type_arc = DISCOUNT_ARCHETYPES_BY_CAMPAIGN.get(campaign_type, DISCOUNT_ARCHETYPES_BY_CAMPAIGN["weekly"])
        archetype = random.choices(
            ["flash", "full", "partial"],
            weights=[type_arc["flash"], type_arc["full"], type_arc["partial"]],
            k=1,
        )[0]

        campaign_seconds = (
            campaign_expired_at - campaign_started_at
        ).total_seconds()

        if archetype == "flash":
            # Launch upon the campaign launch time,
            # then snap it to the nearest realistic peak shopping hour.
            #launch at a random peak hour of the campaign period
            raw_start   = campaign_started_at + datetime.timedelta(
                seconds=random.randint(0, campaign_seconds)
            )
            day_key   = "weekend" if raw_start.weekday() >= 5 else "weekday"
            started_at = Discount._snap_to_peak_hour(
                raw_start,
                PEAK_HOURS.get(day_key, [9, 12, 18, 21]),
                FLASH_DISCOUNT_HOUR_WEIGHTS.get(day_key, [0.25, 0.25, 0.25, 0.25]),
            )

            #Fallback to latest peak hour if discount is expired
            if started_at > campaign_expired_at:
                started_at = Discount._snap_to_peak_hour(
                    campaign_started_at,
                    PEAK_HOURS.get(day_key, [9, 12, 18, 21]),
                    FLASH_DISCOUNT_HOUR_WEIGHTS.get(day_key, [0.25, 0.25, 0.25, 0.25]),
                )


            expired_at = min(
                started_at + datetime.timedelta(minutes=random.randint(30,60)),
                campaign_expired_at,
            )

            
        elif archetype == "full":
            # Discount runs for the entire campaign duration
            started_at = campaign_started_at
            expired_at = campaign_expired_at

        else:  # partial
            # Delayed start (within first 70%), runs through to campaign end
            delay_frac = random.uniform(0.0, 0.70)
            started_at = campaign_started_at + datetime.timedelta(
                seconds=campaign_seconds * delay_frac
            )
            expired_at = campaign_expired_at

        return started_at, expired_at

    @classmethod
    def new(
        cls,
        *,
        discount_type: str,
        adscampaign_id: str,
        campaign_type: str,
        started_at: datetime.datetime,
        expired_at: datetime.datetime,
        fake: Faker,
    ) -> Self:
        """
        Create a new discount tied to an existing campaign.

        The caller is responsible for computing the validity window.
        Use `Discount._window(campaign.started_at, campaign.expired_at)`
        to get a realistic sub-interval automatically.

        Args:
            adscampaign_id: UUID of the parent campaign.
            campaign_started_at: Campaign start (lower bound for discount start).
            campaign_expired_at: Campaign end (upper bound for discount end).
            fake: Faker instance used for UUID generation.
        """
        if discount_type == "percent":
            discount_value = round(random.uniform(MIN_PERCENT_DISCOUNT, MAX_PERCENT_DISCOUNT))
        else:
            discount_value = round(random.uniform(MIN_AMOUNT_DISCOUNT, MAX_AMOUNT_DISCOUNT))

        # Generate a readable promo code: e.g. "SAVE30" or "DEAL12"
        campaign_prefix = campaign_type.upper()
        suffix = "".join(random.choices("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789", k=4))
        discount_code = f"{campaign_prefix}{discount_value}D{started_at.strftime('%m%d%H%M%S')}R{suffix}"

        return cls(
            discount_id=str(uuid.uuid4()),
            adscampaign_id=adscampaign_id,
            discount_type=discount_type,
            discount_value=discount_value,
            discount_code=discount_code,
            started_at=started_at,
            expired_at=expired_at,
        )

    def is_valid(self) -> bool:
        """Check if discount is currently valid (not expired)."""
        now = datetime.datetime.now()
        return self.started_at <= now <= self.expired_at

    def calculate_discount(self, order_amount: float) -> float:
        """Calculate discount amount based on type.
        
        Args:
            order_amount: Base order amount before discount
        
        Returns:
            Discount amount in the same currency
        """
        if self.discount_type == "percent":
            return round(order_amount * (float(self.discount_value) / 100.0), 2)
        elif self.discount_type == "amount":
            return round(float(self.discount_value), 2)
        else:
            return 0.0

    def __str__(self):
        return f"Discount(id={self.discount_id}, type={self.discount_type}, value={self.discount_value})"

    @staticmethod
    def ddl(schema: str = "public"):
        return inspect.cleandoc(f"""
        CREATE TABLE IF NOT EXISTS {schema}.discount
        (
            discount_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            adscampaign_id UUID,
            discount_type VARCHAR(10) CHECK (discount_type IN ('percent', 'amount')),
            discount_value numeric(15,2),
            discount_code VARCHAR(100) UNIQUE,
            started_at TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL,
            expired_at TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL,
            created_at TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL DEFAULT (now() AT TIME ZONE 'Asia/Ho_Chi_Minh'),
            FOREIGN KEY (adscampaign_id) REFERENCES {schema}.adscampaign(campaign_id)
        );
        """)


@dataclasses.dataclass
class Order(ModelMixin):
    order_id: str
    customer_id: str
    address_id: str
    order_amount: float
    order_discount_amount: float
    order_tax_amount: float
    order_total_amount: float
    order_status_id: str
    discount_id: Optional[str]
    payment_method_id: Optional[str]
    payment_status_id: Optional[str]
    shipping_method: int
    shipping_status: int  # Should use ShippingStatus Enum values
    shipped_at: Optional[datetime.datetime] = None
    returned_at: Optional[datetime.datetime] = None

    @classmethod
    def new(
        cls, 
        customer_id: str,
        address_id: str,
        order_items_data: List[dict],
        fake: Faker,
        available_discounts: Optional[List['Discount']] = None,
        payment_method_id: Optional[str] = None,
    ) -> Self:
        """
        Create a new order from product selections with realistic e-commerce modeling.
        
        Real-life behavior:
        - Order amount is calculated from selected products and quantities
        - Tax is applied based on product tax rates (typically 5-20% per item)
        - Discount is applied from available_discounts if provided
        - Shipping method distribution: 70% standard, 30% express
        - Orders start in PENDING shipping status
        
        Note: The caller (generator.py) checks discount probability (30%) FIRST before querying
        the database for available discounts. This minimizes unnecessary database queries.
        
        Args:
            customer_id: ID of the customer placing the order
            address_id: ID of the shipping address
            order_items_data: List of dicts with 'product_id', 'price', 'tax', 'quantity'
            order_status_id: Initial order status ID (typically PROCESSING)
            fake: Faker instance
            available_discounts: Optional list of active Discount objects (only passed if probability check passed)
            payment_method_id: Optional payment method UUID
            payment_status_id: Optional payment status UUID
            
        Returns:
            Order instance with calculated totals and optionally applied discount
        """
        if fake is None:
            fake = Faker()
        
        # Calculate order subtotal (before tax and discount)
        order_amount = 0.0
        order_tax_amount = 0.0
        
        for item_data in order_items_data:
            # order_amount excludes tax (sum of price * quantity)
            item_subtotal = item_data["price"] * item_data["quantity"]
            # Calculate tax on this item based on product_tax field
            item_tax = item_subtotal * (item_data["tax"] / 100.0)
            
            order_amount += item_subtotal
            order_tax_amount += item_tax
        
        # Apply discount from available campaigns
        # Note: Probability check is done in generator.py before querying discounts,
        # so we only reach here if a discount should be considered
        order_discount_amount = 0.0
        discount_id = None
        
        if available_discounts and len(available_discounts) > 0:
            # Randomly select a discount from available active discounts
            selected_discount = fake.random_element(available_discounts)
            discount_id = selected_discount.discount_id
            order_discount_amount = selected_discount.calculate_discount(order_amount)
        
        # Total order amount: subtotal + tax - discount
        order_total_amount = order_amount + order_tax_amount - order_discount_amount
        
        # Realistic shipping method distribution from catalog
        shipping_method = fake.random_choices(
            elements=SHIPPING_METHOD_DISTRIBUTION,
            length=1,
        )[0]
        
        return cls(
            order_id=str(uuid.uuid4()),
            customer_id=customer_id,
            address_id=address_id,
            order_amount=round(order_amount, 2),
            order_discount_amount=round(order_discount_amount, 2),
            order_tax_amount=round(order_tax_amount, 2),
            order_total_amount=round(order_total_amount, 2),
            order_status_id=OrderStatus.PROCESSING.value,
            discount_id=discount_id,
            payment_method_id=payment_method_id,
            payment_status_id=PaymentStatus.PENDING.value,
            shipping_method=shipping_method,
            shipping_status=ShippingStatus.PENDING.value,
            shipped_at=None,
            returned_at=None,
        )

    def __str__(self):
        return f"Order(id={self.order_id}, customer_id={self.customer_id}, status_id={self.order_status_id}, total={self.order_total_amount})"

    @staticmethod
    def ddl(schema: str):
        return inspect.cleandoc(f"""
        CREATE TABLE IF NOT EXISTS {schema}.order (
            order_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            customer_id UUID NOT NULL,
            address_id UUID NOT NULL,
            order_amount decimal(15,2) NOT NULL,
            order_discount_amount decimal(15,2) NOT NULL,
            order_tax_amount decimal(15,2) NOT NULL,
            order_total_amount decimal(15,2) NOT NULL,
            discount_id UUID,
            payment_method_id INTEGER,
            payment_status_id INTEGER,
            order_status_id INTEGER NOT NULL,
            shipping_method INTEGER,
            shipping_status INTEGER,
            shipped_at TIMESTAMP(0) WITHOUT TIME ZONE,
            returned_at TIMESTAMP(0) WITHOUT TIME ZONE,
            created_at TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL DEFAULT (now() AT TIME ZONE 'Asia/Ho_Chi_Minh'),
            updated_at TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL DEFAULT (now() AT TIME ZONE 'Asia/Ho_Chi_Minh'),
            FOREIGN KEY (customer_id) REFERENCES {schema}.customer(customer_id),
            FOREIGN KEY (address_id) REFERENCES {schema}.address(address_id)
            -- FOREIGN KEY (order_status_id) REFERENCES {schema}.order_status(order_status_id)
        );
        """)


@dataclasses.dataclass
class Transaction(ModelMixin):
    """
    Financial transaction linked to an order.

    Two types:
    - payment : created after order placement (online) or after delivery (COD).
    - refund  : created after the order is returned.

    transaction_status=True  → success
    transaction_status=False → failed (online payment only)
    """
    order_id: str
    transaction_type: str       # 'payment' | 'refund'
    transaction_amount: float
    transaction_status: bool
    description: Optional[str]

    @classmethod
    def new(
        cls,
        *,
        order_id: str,
        transaction_type: str,
        amount: float,
        success: bool = True,
        description: Optional[str] = None,
    ) -> Self:
        return cls(
            order_id=order_id,
            transaction_type=transaction_type,
            transaction_amount=round(amount, 2),
            transaction_status=success,
            description=description,
        )

    @staticmethod
    def ddl(schema: str) -> str:
        return inspect.cleandoc(f"""
        CREATE TABLE IF NOT EXISTS {schema}.transaction (
            transaction_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            order_id           UUID REFERENCES {schema}.order(order_id),
            transaction_type   VARCHAR(10) CHECK (transaction_type IN ('payment', 'refund')),
            transaction_amount NUMERIC(15, 2),
            transaction_status BOOLEAN DEFAULT FALSE, -- true: success, false: failed
            description        TEXT,
            created_at         TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL DEFAULT (now() AT TIME ZONE 'Asia/Ho_Chi_Minh')
        );
        """)


@dataclasses.dataclass
class OrderStatusHistory(ModelMixin):
    """Append-only audit trail for order status changes.
    
    This table tracks all status transitions for an order and is immutable after insertion.
    It provides a complete history of how an order progressed through different states.
    
    Real-world order status transitions:
    - Processing: Payment processing and order validation (1-3 days)
    - Shipped: Order picked, packed, and handed to carrier (within 1 day)
    - Delivered: Package successfully delivered to customer (3-7 days)
    - Cancelled: Order cancelled before shipment (any point)
    - Returned: Package returned by customer (within 14-30 days after delivery)
    
    Database constraints:
    - UNIQUE(order_id, order_status_id): Ensures each status appears only once per order
    - This enforces the business rule that an order cannot transition to the same status twice
    """
    order_id: str
    order_status_id: str
    comments: Optional[str] = None

    @classmethod
    def new(
        cls,
        order_id: str,
        order_status_id: str,
        fake: Faker,
        comments: Optional[str] = None
    ) -> Self:
        """
        Create a new order status history record (append-only).
        
        This creates an immutable audit log entry for an order status change.
        Each status transition is timestamped and optionally annotated with comments
        describing the reason for the change or relevant details.
        
        Common status transitions and their typical timelines:
        - Processing → Shipped: 1-3 days after order
        - Shipped → Delivered: 3-7 days depending on shipping method
        - At any point: Cancelled (if customer requests)
        - After delivery: Returned (within return window, typically 14-30 days)
        
        Args:
            order_id: UUID of the order this status change applies to
            order_status_id: UUID of the new order status (from order_status table)
            comments: Optional context about the status change (e.g., \"Shipped via FedEx\", \"Returned due to damage\")
            fake: Faker instance (creates one if None)
        
        Returns:
            OrderStatusHistory instance representing this state transition
        """

        
        return cls(
            order_id=order_id,
            order_status_id=order_status_id,
            comments=fake.sentence() if comments is None else comments,
        )

    def __str__(self):
        return f"OrderStatusHistory(order_id={self.order_id}, status_id={self.order_status_id})"

    @staticmethod
    def ddl(schema: str):
        return inspect.cleandoc(f"""
        CREATE TABLE IF NOT EXISTS {schema}.order_status_history (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            order_id UUID NOT NULL,
            order_status_id INTEGER NOT NULL,
            comments VARCHAR(500),
            created_at TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL DEFAULT (now() AT TIME ZONE 'Asia/Ho_Chi_Minh'),
            UNIQUE(order_id, order_status_id),
            FOREIGN KEY (order_id) REFERENCES {schema}.order(order_id),
            FOREIGN KEY (order_status_id) REFERENCES {schema}.order_status(order_status_id)
        );
        """)


@dataclasses.dataclass
class OrderItem(ModelMixin):
    """Order line item representing a product in an order (maps to order_detail table)."""
    order_id: str
    product_id: str
    order_quantity: int
    order_product_price: float
    order_product_tax: float
    order_subtotal_amount: float

    @classmethod
    def new(
        cls, 
        order_id: str, 
        product_id: str,
        product_price: float,
        product_tax: float,
        fake: Faker,
        quantity: int = 1,
    ) -> Self:
        """
        Create a new order item (order_detail record) for a product in an order.
        
        Calculation methodology:
        - order_subtotal_amount = product_price * quantity (subtotal before tax)
        - order_product_tax = subtotal * (product_tax / 100) (tax amount for this line item)
        - order_product_price stores the unit price at time of purchase (for historical accuracy)
        
        Args:
            order_id: UUID of the order this item belongs to
            product_id: UUID of the product being ordered
            product_price: Unit price of the product at time of purchase
            product_tax: Tax rate percentage for the product (typically 5-20%)
            quantity: Number of units ordered (typically 1-5 in e-commerce)
            fake: Faker instance
        
        Returns:
            OrderItem instance with calculated tax and subtotal amounts
        """
        if fake is None:
            fake = Faker()
        
        # Calculate subtotal (price * quantity) before tax
        subtotal = product_price * quantity
        
        # Calculate tax amount (subtotal * tax_rate%)
        tax_amount = subtotal * (product_tax / 100.0)
        
        return cls(
            order_id=order_id,
            product_id=product_id,
            order_quantity=quantity,
            order_product_price=round(product_price, 2),  # Unit price at time of purchase
            order_product_tax=round(tax_amount, 2),  # Tax for this line item
            order_subtotal_amount=round(subtotal, 2),  # Subtotal before tax
        )

    def __str__(self):
        return f"OrderItem(id={self.order_detail_id}, order_id={self.order_id}, product_id={self.product_id}, qty={self.order_quantity}, price={self.order_product_price})"

    @staticmethod
    def ddl(schema: str):
        return inspect.cleandoc(f"""
        CREATE TABLE IF NOT EXISTS {schema}.order_detail (
            order_detail_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            order_id UUID NOT NULL,
            product_id UUID NOT NULL,
            order_quantity INTEGER NOT NULL DEFAULT 1,
            order_product_price numeric(15, 2) NOT NULL,
            order_product_tax numeric(15, 2) NOT NULL,
            order_subtotal_amount numeric(15, 2) NOT NULL,
            created_at TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL DEFAULT (now() AT TIME ZONE 'Asia/Ho_Chi_Minh'),
            FOREIGN KEY (order_id) REFERENCES {schema}.order(order_id),
            FOREIGN KEY (product_id) REFERENCES {schema}.product(product_id)
        );
        """)


# @dataclasses.dataclass
# class Event(ModelMixin):
#     id: str
#     user_id: Optional[str]
#     sequence_number: int
#     session_id: str
#     ip_address: str
#     city: str
#     state: str
#     postal_code: str
#     browser: str
#     traffic_source: str
#     uri: str
#     event_type: str
#     created_at: datetime.datetime

#     @classmethod
#     def new(
#         cls,
#         user: Optional[User],
#         order_item: Optional[OrderItem],
#         event_category: str,
#         fake: Faker,
#     ) -> List[Self]:
            
#         if event_category in ["purchase", "return", "cancel"]:
#             assert order_item is not None
#             user_id = user.id
#             city = fake.city()
#             state = fake.state()
#             postal_code = fake.postcode()
#             product_id = order_item.product_id
#             order_item_id = order_item.id
#             if event_category == "purchase":
#                 created_at = order_item.created_at
#                 event_types = list(
#                     set(fake.random_choices(["home", "department", "product"], 3))
#                 ) + ["cart", "purchase"]
#             else:
#                 created_at = datetime.datetime.now()
#                 event_types = ["product", "cart", event_category]
#         elif event_category == "ghost":
#             # Ghost events: browsing without purchasing
#             user_id = None
#             city = fake.city()
#             state = fake.state()
#             postal_code = fake.postcode()
#             product_id = str(uuid.uuid4())  # Random product ID for ghost browsing
#             order_item_id = None
#             created_at = datetime.datetime.now()
#             event_types = fake.random_elements(
#                 [
#                     "home",
#                     "department",
#                     "category",
#                     "product",
#                     "cart",
#                 ],
#                 length=fake.random_element(range(3, 6)),
#             )
#         else:
#             raise RuntimeError(
#                 f"Unsupported event category: '{event_category}'. Allowed categories are: {', '.join(sorted(['purchase', 'cancel', 'return', 'ghost']))}."
#             )
#         session_id = str(uuid.uuid4())
#         ip_address = fake.ipv4()
#         browser = fake.random_choices(
#             elements=BROWSER_DISTRIBUTION,
#             length=1,
#         )[0]
#         traffic_source = fake.random_choices(
#             elements=EVENT_TRAFFIC_SOURCES,
#             length=1,
#         )[0]
#         events = [
#             cls(
#                 id=str(uuid.uuid4()),
#                 user_id=user_id,
#                 sequence_number=idx + 1,
#                 session_id=session_id,
#                 ip_address=ip_address,
#                 city=city,
#                 state=state,
#                 postal_code=postal_code,
#                 browser=browser,
#                 traffic_source=traffic_source,
#                 uri=cls._generate_uri(event_type, order_item_id, product_id),
#                 event_type=event_type,
#                 created_at=created_at
#                 - cls._calculate_event_delay(len(event_types), idx, fake),
#             )
#             for idx, event_type in enumerate(event_types)
#         ]
#         return events

#     def __str__(self):
#         return f"Event(id={self.id}, is_ghost={self.user_id is None}, sequence_number={self.sequence_number}, event_type={self.event_type}, created_at={self.created_at})"

#     @staticmethod
#     def _generate_uri(event_type: str, item_id: Optional[str], product_id: str) -> str:
#         if event_type == "product":
#             return f"/{event_type}/{product_id}"
#         elif event_type == "department":
#             # Generate dummy department and category for department pages
#             departments = ["electronics", "home", "fashion", "sports", "books"]
#             categories = ["subcategory1", "subcategory2", "subcategory3"]
#             return f"/{event_type}/{random.choice(departments)}/category/{random.choice(categories)}"
#         elif event_type in ["cancel", "return"]:
#             return (
#                 f"/{event_type}/item/{item_id}"
#                 if item_id is not None
#                 else f"/{event_type}"
#             )
#         else:
#             return f"/{event_type}"

#     @staticmethod
#     def _calculate_event_delay(
#         num_events: int, idx: int, fake: Faker
#     ) -> datetime.timedelta:
#         if num_events == idx + 1:
#             return datetime.timedelta(seconds=0)
#         base_delay = (num_events - idx + 1) * 20
#         jitter = fake.random_element(range(1, 10))
#         return datetime.timedelta(seconds=base_delay + jitter)

#     @staticmethod
#     def ddl(schema: str):
#         return inspect.cleandoc(f"""
#         CREATE TABLE IF NOT EXISTS {schema}.events (
#             id                  TEXT PRIMARY KEY,
#             user_id             TEXT,
#             sequence_number     INT,
#             session_id          TEXT,
#             ip_address          TEXT,
#             city                TEXT,
#             state               TEXT,
#             postal_code         TEXT,
#             browser             TEXT,
#             traffic_source      TEXT,
#             uri                 TEXT,
#             event_type          TEXT,
#             created_at          TIMESTAMP WITHOUT TIME ZONE
#         );
#         """)

@dataclasses.dataclass
class Address(ModelMixin):

    address_id: str
    address_type: str
    customer_id: str
    province_id: int           # FK → province.province_id (SERIAL/INTEGER)
    address_full: str

    @classmethod
    def new(
        cls,
        *,
        customer_id: str,
        province_id: str,
        fake: Faker,
    ) -> Self:
        address_type = fake.random_choices(
            elements=ADDRESS_TYPE_DISTRIBUTION, length=1
        )[0]
        
        return cls(
            address_id=str(uuid.uuid4()),
            address_type=address_type,
            customer_id=customer_id,
            province_id=province_id,
            address_full=f"{fake.street_address()}, {fake.secondary_address()}",
        )
        
    @staticmethod
    def ddl(schema: str = "public"):
        return inspect.cleandoc(f"""
        CREATE TABLE IF NOT EXISTS {schema}.address
        (
            address_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            address_type VARCHAR(100) NOT NULL,
            customer_id UUID NOT NULL,
            province_id INTEGER NOT NULL,
            address_full VARCHAR(255),
            created_at TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL DEFAULT (now() AT TIME ZONE 'Asia/Ho_Chi_Minh'),
            
            FOREIGN KEY (customer_id) REFERENCES {schema}.customer(customer_id),
            FOREIGN KEY (province_id) REFERENCES {schema}.province(province_id),

            CHECK (address_type IN ('home', 'office'))
        );
        """)

@dataclasses.dataclass
class Brand(ModelMixin):
    brand_id: str
    brand_name: str

    @classmethod
    def new(
        cls,
        *,
        brand_name: str,
        fake: Faker,
    ) -> Self:
        return cls(
            brand_id=str(uuid.uuid4()),
            brand_name=brand_name,
        )
    
    @staticmethod
    def ddl(schema: str = "public"):
        return inspect.cleandoc(f"""
        CREATE TABLE IF NOT EXISTS {schema}.brand
        (
            brand_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            brand_name VARCHAR(100) NOT NULL UNIQUE,
            created_at TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL DEFAULT (now() AT TIME ZONE 'Asia/Ho_Chi_Minh')
        );
        """)

@dataclasses.dataclass
class Category(ModelMixin):
    category_id: str
    category_name: str
    parent_category_id: Optional[str]

    @classmethod
    def new(
        cls,
        *,
        category_name: str,
        fake: Faker,
        parent_category_id: Optional[str] = None,
    ) -> Self:
        return cls(
            category_id=str(uuid.uuid4()),
            category_name=category_name,
            parent_category_id=parent_category_id,
        )
    
    @staticmethod
    def ddl(schema: str = "public"):
        return inspect.cleandoc(f"""
        CREATE TABLE IF NOT EXISTS {schema}.category
        (
            category_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            category_name VARCHAR(100) NOT NULL UNIQUE,
            parent_category_id UUID,
            created_at TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL DEFAULT (now() AT TIME ZONE 'Asia/Ho_Chi_Minh'),
            
            FOREIGN KEY (parent_category_id) REFERENCES {schema}.category(category_id)
        );
        """)


@dataclasses.dataclass
class Product:
    """Product model for electronics e-commerce platform"""

    product_id: str
    sku: str
    product_name: str
    category_id: str
    brand_id: str
    product_description: str
    product_price: Decimal
    product_unit_cost: Decimal
    product_tax: Decimal
    product_quantity: int
    product_image_path: Optional[str]

    @classmethod
    def new(
        cls,
        *,
        category_id: str,
        brand_id: str,
        fake: Faker,
        category_name: str,
        subcategory: str,
        brand_name: str,
    ) -> Self:
        """
        Generate a new Product instance with realistic electronics data.

        Args:
            category_id: UUID of the category
            brand_id: UUID of the brand
            fake: Faker instance for data generation
            category_name: Human-readable category name
            subcategory: Product subcategory
            brand_name: Brand name for this product

        Returns:
            A new Product instance with generated data
        """
        # Generate realistic pricing based on category
        unit_cost = cls._generate_cost(subcategory, fake)
        markup = fake.random.uniform(1.35, 2.5)  # 35-150% markup
        selling_price = Decimal(str(round(unit_cost * markup, 2)))
        tax_rate = Decimal(str(fake.random.uniform(0.05, 0.15)))  # 5-15% tax

        # Generate SKU
        sku = cls._generate_sku(brand_name, subcategory, fake)

        # Generate product name
        product_name = cls._generate_product_name(subcategory, brand_name, fake)

        # Generate product description
        description = cls._generate_description(
            subcategory, brand_name, product_name, fake
        )

        # Generate image path (realistic format)
        image_path = fake.image_url()

        # Generate quantity in stock
        quantity = fake.random_int(min=0, max=500)

        return cls(
            product_id=str(uuid.uuid4()),
            sku=sku,
            product_name=product_name,
            category_id=category_id,
            brand_id=brand_id,
            product_description=description,
            product_price=selling_price,
            product_unit_cost=Decimal(str(round(unit_cost, 2))),
            product_tax=tax_rate,
            product_quantity=quantity,
            product_image_path=image_path,
        )

    @staticmethod
    def _generate_cost(subcategory: str, fake: Faker) -> float:
        """Generate realistic unit cost based on product category"""
        min_cost, max_cost = PRODUCT_COST_RANGE.get(subcategory, (50, 500))
        return fake.random.uniform(min_cost, max_cost)

    @staticmethod
    def _generate_sku(brand_name: str, subcategory: str, fake: Faker) -> str:
        """Generate unique SKU following standard format"""
        brand_prefix = brand_name[:3].upper()
        category_code = subcategory[:3].upper()
        random_suffix = fake.bothify(text="??-####")
        return f"{brand_prefix}-{category_code}-{random_suffix}"

    @staticmethod
    def _generate_product_name(subcategory: str, brand_name: str, fake: Faker) -> str:
        """Generate realistic product names"""
        if subcategory in PRODUCT_TEMPLATES:
            template = fake.random_element(PRODUCT_TEMPLATES[subcategory])
            specs = PRODUCT_SPECS.get(subcategory, ["Pro", "Plus", "Ultra"])
            spec = fake.random_element(specs)

            models = ["X", "Pro", "Max", "Plus", "Ultra", "SE", "Lite"]
            model = fake.random_element(models)

            return template.format(
                brand=brand_name,
                series=fake.random_element(["Series", "Gen", "Edition"]),
                model=model,
                spec=spec,
            )
        else:
            # Fallback for categories without templates
            variant = fake.random_element(["Pro", "Plus", "Max", "Standard"])
            return f"{brand_name} {subcategory} {variant}"

    @staticmethod
    def _generate_description(
        subcategory: str, brand_name: str, product_name: str, fake: Faker
    ) -> str:
        """Generate realistic product descriptions"""
        descriptions = {
            "Smartphones": [
                f"Premium smartphone featuring advanced camera technology, high refresh rate display, and powerful processor. Experience lightning-fast performance with {brand_name}'s latest innovation.",
                f"{product_name} offers cutting-edge features including HDR display, extended battery life, and 5G connectivity for seamless connectivity.",
            ],
            "Laptops": [
                f"Lightweight yet powerful laptop perfect for professionals and creators. Features impressive performance, stunning display, and all-day battery life.",
                f"{product_name} combines portability with performance, ideal for multitasking and demanding applications.",
            ],
            "Televisions (LED, OLED, QLED)": [
                f"Stunning picture quality with vibrant colors and deep blacks. Immerse yourself in your favorite content with this {brand_name} television.",
                f"Smart TV with built-in streaming apps, excellent refresh rate, and advanced color technology for cinematic viewing experience.",
            ],
            "Air Conditioners": [
                f"Efficient cooling solution with smart temperature control and energy-saving features. Keep your space comfortable all year round.",
                f"{brand_name} air conditioner with whisper-quiet operation, rapid cooling, and advanced filtration system.",
            ],
            "Washing Machines": [
                f"Fully automatic washing machine with multiple wash programs and energy efficiency. Gentle on fabrics, tough on stains.",
                f"Smart washing machine featuring quick wash cycles, large capacity, and advanced water-saving technology.",
            ],
            "Coffee Machines": [
                f"Professional-grade coffee maker for coffee enthusiasts. Create barista-quality drinks at home with precision brewing.",
                f"{brand_name} coffee machine with easy operation, consistent quality, and minimal cleanup required.",
            ],
            "Smart Speakers": [
                f"Voice-controlled smart speaker with crystal-clear audio quality. Connect to your smart home devices seamlessly.",
                f"Experience premium sound with this intelligent speaker featuring voice assistance and smart home integration.",
            ],
            "Vacuum Cleaners": [
                f"Powerful suction cleaning for all surfaces. Lightweight and maneuverable for effortless home cleaning.",
                f"{brand_name} vacuum with advanced filtration, quiet operation, and versatile cleaning attachments.",
            ],
        }

        category_desc = descriptions.get(
            subcategory,
            [
                f"High-quality {subcategory.lower()} by {brand_name}. Professional grade with advanced features and reliable performance.",
                f"{product_name} offers exceptional value with modern features and dependable operation.",
            ],
        )

        base_desc = fake.random_element(category_desc)
        warranty = fake.random_element(["1-year", "2-year", "3-year"])
        support = fake.random_element(["24/7 customer support", "dedicated support team", "comprehensive warranty"])

        return f"{base_desc} Includes {warranty} warranty and {support}."

    @staticmethod
    def ddl(schema: str = "public") -> str:
        """Generate DDL for product table"""
        return inspect.cleandoc(
            f"""
        CREATE TABLE IF NOT EXISTS {schema}.product
        (
            product_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            sku VARCHAR(100) NOT NULL UNIQUE,
            product_name VARCHAR(255) NOT NULL,
            category_id UUID NOT NULL,
            brand_id UUID NOT NULL,
            product_description TEXT,
            product_price NUMERIC(15,2) NOT NULL,
            product_unit_cost NUMERIC(15,2) NOT NULL DEFAULT 0,
            product_tax NUMERIC(4,2) NOT NULL DEFAULT 0,
            product_quantity INTEGER NOT NULL DEFAULT 0,
            product_image_path VARCHAR,
            created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (now() AT TIME ZONE 'Asia/Ho_Chi_Minh'),

            FOREIGN KEY (category_id) REFERENCES {schema}.category(category_id),
            FOREIGN KEY (brand_id) REFERENCES {schema}.brand(brand_id),

            CHECK (product_price >= 0),
            CHECK (product_unit_cost >= 0),
            CHECK (product_tax >= 0 AND product_tax <= 100),
            CHECK (product_quantity >= 0)
        );
        """
        )
