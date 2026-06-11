"""
Constants and Enums for the e-commerce lakehouse database models.
"""
from enum import Enum


class OrderStatus(Enum):
    """Order status enumeration."""
    PROCESSING = 1
    SHIPPED = 2
    DELIVERED = 3
    CANCELLED = 4
    RETURNED = 5


class Region(Enum):
    """Geographic region enumeration."""
    NORTH = 1
    CENTRAL = 2
    SOUTH = 3


class EventCategory(Enum):
    """Event category enumeration."""
    PURCHASE = "purchase"
    GHOST = "ghost"
    CANCEL = "cancel"
    RETURN = "return"


class PaymentMethod(Enum):
    """Payment method enumeration."""
    CREDIT_CARD = 1
    DEBIT_CARD = 2
    PAYPAL = 3
    BANK_TRANSFER = 4
    CASH_ON_DELIVERY = 5


class PaymentStatus(Enum):
    """Payment status enumeration."""
    PENDING = 1
    COMPLETED = 2
    FAILED = 3
    REFUNDED = 4


class ShippingStatus(Enum):
    """Shipping status enumeration."""
    PENDING = 1
    IN_TRANSIT = 2
    OUT_FOR_DELIVERY = 3
    DELIVERED = 4
    RETURNED = 6


class TransactionType(Enum):
    """Transaction type enumeration — matches enum_transaction_type in setup.sql."""
    PAYMENT = "payment"
    REFUND  = "refund"
