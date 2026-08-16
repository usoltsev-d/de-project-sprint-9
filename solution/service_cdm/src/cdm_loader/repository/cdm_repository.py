from lib.pg import PgConnect


class CdmRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def save_order(self, message: dict) -> None:
        user_id = message['user_id']
        products = message['products']

        # Одно блюдо считаем один раз на заказ
        unique_products = {}

        # Одну категорию считаем один раз на заказ
        unique_categories = {}

        for product in products:
            unique_products[product['product_id']] = product
            unique_categories[product['category_id']] = product

        # Весь заказ обновляем одной транзакцией
        with self._db.connection() as conn:
            with conn.cursor() as cur:

                for product in unique_products.values():
                    self._upsert_user_product_counter(
                        cur,
                        user_id,
                        product['product_id'],
                        product['product_name']
                    )

                for product in unique_categories.values():
                    self._upsert_user_category_counter(
                        cur,
                        user_id,
                        product['category_id'],
                        product['category_name']
                    )

    def _upsert_user_product_counter(
        self,
        cur,
        user_id: str,
        product_id: str,
        product_name: str
    ) -> None:
        cur.execute(
            """
            INSERT INTO cdm.user_product_counters
            (
                user_id,
                product_id,
                product_name,
                order_cnt
            )
            VALUES
            (
                %(user_id)s,
                %(product_id)s,
                %(product_name)s,
                1
            )
            ON CONFLICT (user_id, product_id)
            DO UPDATE
            SET
                product_name = EXCLUDED.product_name,
                order_cnt = cdm.user_product_counters.order_cnt + 1
            """,
            {
                'user_id': user_id,
                'product_id': product_id,
                'product_name': product_name
            }
        )

    def _upsert_user_category_counter(
        self,
        cur,
        user_id: str,
        category_id: str,
        category_name: str
    ) -> None:
        cur.execute(
            """
            INSERT INTO cdm.user_category_counters
            (
                user_id,
                category_id,
                category_name,
                order_cnt
            )
            VALUES
            (
                %(user_id)s,
                %(category_id)s,
                %(category_name)s,
                1
            )
            ON CONFLICT (user_id, category_id)
            DO UPDATE
            SET
                category_name = EXCLUDED.category_name,
                order_cnt = cdm.user_category_counters.order_cnt + 1
            """,
            {
                'user_id': user_id,
                'category_id': category_id,
                'category_name': category_name
            }
        )