import logging
import asyncio
import functools

from sqlalchemy.exc import SQLAlchemyError

logging.basicConfig(
    level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s"
)


def async_retry_on_db_error(max_retries=3, initial_delay=1):
    """
    A decorator factory that makes an async function retry on SQLAlchemyError.

    Args:
        max_retries (int): Maximum number of retries before giving up.
        initial_delay (int): The initial delay in seconds for the first retry.
                             The delay uses exponential backoff.
    """

    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            for attempt in range(1, max_retries + 1):
                try:
                    # Attempt to run the decorated async function
                    return await func(*args, **kwargs)
                except SQLAlchemyError as e:
                    if attempt == max_retries:
                        logging.error(
                            f"DB operation '{func.__name__}' failed after {max_retries} attempts. Final error: {e}"
                        )
                        raise  # Re-raise the last exception to exit the program

                    # Calculate delay with exponential backoff
                    delay = initial_delay * (2 ** (attempt - 1))
                    logging.warning(
                        f"DB error in '{func.__name__}' on attempt {attempt}/{max_retries}. "
                        f"Retrying in {delay}s... Error: {e}"
                    )
                    await asyncio.sleep(delay)

        return wrapper

    return decorator
