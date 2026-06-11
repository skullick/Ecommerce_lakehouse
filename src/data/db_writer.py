import dataclasses
import inspect
import logging
from typing import List, Union, Optional
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection
from sqlalchemy.exc import OperationalError
from psycopg2.extras import execute_values

from data.models import (
    User, Order, OrderItem, Address,
    Adscampaign, Discount,
    Brand, Category, Product,
    Transaction,
    get_additional_ddls,
)

logging.basicConfig(
    level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s"
)


class DataWriter:
    def __init__(
        self,
        user: str,
        password: str,
        host: str,
        db_name: str,
        schema: str,
        batch_size: int = 1000,
        echo: bool = False,
    ):
        self.schema = schema
        self.batch_size = batch_size
        self.echo = echo
        self._user = user
        self._password = password
        self._host = host
        self._db_name = db_name
        self._connect()

    def select(
        self,
        table: str,
        columns: Optional[List[str]] = None,
        where_clause: Optional[str] = None,
        where_params: Optional[tuple] = None,
        order_by: Optional[str] = None,
        limit: Optional[int] = None,
    ):
        cols = ", ".join(columns) if columns else "*"
        sql = f"SELECT {cols} FROM {self.schema}.{table}"
        if where_clause:
            sql += f" WHERE {where_clause}"
        if order_by:
            sql += f" ORDER BY {order_by}"
        if limit is not None:
            sql += f" LIMIT {limit}"

        with self.engine.connect() as conn:
            result = conn.execute(text(sql), where_params or {})
            return result.mappings().all()

    def upsert(
        self,
        table: str,
        data: List[Union[dict, object]],
        conflict_keys: Optional[List[str]] = None,
        update_fields: Optional[List[str]] = None,
    ):
        if not data:
            return

        rows = self._normalize_rows(data)
        columns = list(rows[0].keys())
        insert_cols = ", ".join(columns)
        remain_cols = [col for col in columns if col not in (conflict_keys or [])]

        if conflict_keys:
            if remain_cols:
                if update_fields is None:
                    update_fields = remain_cols
                
                # Automatically include updated_at if it's not in the update_fields
                # and we know the table has it (based on the columns being inserted)
                update_items = [f"{col} = EXCLUDED.{col}" for col in update_fields]
                if "updated_at" in columns and "updated_at" not in update_fields:
                    update_items.append("updated_at = EXCLUDED.updated_at")
                
                update_clause = ", ".join(update_items)
                conflict_clause = ", ".join(conflict_keys)
                on_conflict_clause = f"ON CONFLICT ({conflict_clause}) DO UPDATE SET {update_clause}"
                logging.debug(f"Upserting to {table} with ON CONFLICT: {update_clause}")
            else:
                conflict_clause = ", ".join(conflict_keys)
                on_conflict_clause = f"ON CONFLICT ({conflict_clause}) DO NOTHING"
        else:
            on_conflict_clause = ""

        sql = f"INSERT INTO {self.schema}.{table} ({insert_cols}) VALUES %s {on_conflict_clause}"
        
        with self.engine.connect() as conn:
            with conn.connection.cursor() as cur:
                try:
                    for i in range(0, len(rows), self.batch_size):
                        batch = rows[i : i + self.batch_size]
                        values = [tuple(row[col] for col in columns) for row in batch]
                        execute_values(cur, sql, values)
                    conn.connection.commit()
                    logging.info(f"[{table}] Inserted {len(rows)} rows.")
                except Exception as e:
                    conn.connection.rollback()
                    logging.error(f"Upsert failed for {table}: {e}")
                    raise

    def get_all_tables(self):
        return [table for table, _ in self._ddl_stmts().items()]

    def create_tables_if_not_exists(self):
        with self.engine.connect() as conn:
            # Ensure the target schema exists before creating any tables.
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {self.schema}"))
            conn.connection.commit()
            for table, ddl in self._ddl_stmts().items():
                logging.info(f"Creating table {self.schema}.{table}")
                conn.execute(text(ddl))
            conn.connection.commit()

    def truncate(self, table: str):
        with self.engine.connect() as conn:
            logging.info(f"Truncating table {self.schema}.{table}")
            conn.execute(text(f"TRUNCATE TABLE {self.schema}.{table} RESTART IDENTITY CASCADE"))
            conn.connection.commit()

    def truncate_all_tables(self):
        """Truncate all managed tables in a single transaction for speed."""
        tables = self.get_all_tables()
        if not tables:
            return
            
        full_table_names = ", ".join([f"{self.schema}.{t}" for t in tables])
        logging.info(f"Truncating all tables in {self.schema}...")
        with self.engine.connect() as conn:
            conn.execute(text(f"TRUNCATE TABLE {full_table_names} RESTART IDENTITY CASCADE"))
            conn.connection.commit()

    def close(self):
        if self.engine:
            self.engine.dispose()

    def _ddl_stmts(self):
        extra = get_additional_ddls(self.schema)
        s = self.schema
        return {
            "customer":       User.ddl(s),
            "province":       extra["province"],
            "brand":          Brand.ddl(s),
            "category":       Category.ddl(s),
            "product":        Product.ddl(s),
            "address":        Address.ddl(s),
            # Lookup tables — created and seeded together
            "order_status": inspect.cleandoc(f"""
                CREATE TABLE IF NOT EXISTS {s}.order_status (
                    order_status_id   SERIAL PRIMARY KEY,
                    order_status_name VARCHAR(100) NOT NULL UNIQUE,
                    created_at        TIMESTAMP(0) NOT NULL DEFAULT (now() AT TIME ZONE 'Asia/Ho_Chi_Minh')
                );
            """),
            "payment_status": inspect.cleandoc(f"""
                CREATE TABLE IF NOT EXISTS {s}.payment_status (
                    payment_status_id   SERIAL PRIMARY KEY,
                    payment_status_name VARCHAR(100) NOT NULL UNIQUE,
                    created_at          TIMESTAMP(0) NOT NULL DEFAULT (now() AT TIME ZONE 'Asia/Ho_Chi_Minh')
                );
            """),
            "payment_method": inspect.cleandoc(f"""
                CREATE TABLE IF NOT EXISTS {s}.payment_method (
                    payment_method_id   SERIAL PRIMARY KEY,
                    payment_method_name VARCHAR(100) NOT NULL UNIQUE,
                    created_at          TIMESTAMP(0) NOT NULL DEFAULT (now() AT TIME ZONE 'Asia/Ho_Chi_Minh')
                );
            """),
            "adscampaign":    Adscampaign.ddl(s),
            "discount":       Discount.ddl(s),
            "order":          Order.ddl(s),
            "order_detail":     OrderItem.ddl(s),
            "order_status_history": inspect.cleandoc(f"""
                CREATE TABLE IF NOT EXISTS {s}.order_status_history (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    order_id UUID NOT NULL,
                    order_status_id  INTEGER NOT NULL,
                    comments VARCHAR(500),	
                    created_at        TIMESTAMP(0) NOT NULL DEFAULT (now() AT TIME ZONE 'Asia/Ho_Chi_Minh'),
                    UNIQUE(order_id, order_status_id),	
	                FOREIGN KEY (order_id) REFERENCES {s}.order(order_id),
                    FOREIGN KEY (order_status_id) REFERENCES {s}.order_status(order_status_id)
                );
            """),
            "transaction":    Transaction.ddl(s),
            "heartbeat":      extra["heartbeat"],
        }

    def _connect(self):
        self.engine = create_engine(
            f"postgresql+psycopg2://{self._user}:{self._password}@{self._host}/{self._db_name}",
            echo=self.echo,
            pool_size=20,
            max_overflow=0,
        )

    @staticmethod
    def _normalize_rows(data: List[Union[dict, object]]) -> List[dict]:
        if not data:
            return []
        if dataclasses.is_dataclass(data[0]):
            return [dataclasses.asdict(obj) for obj in data]
        if isinstance(data[0], dict):
            return data
        raise TypeError("Each item must be a dataclass or dict.")
