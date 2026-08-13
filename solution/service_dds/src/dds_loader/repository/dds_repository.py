import uuid
from datetime import datetime

from lib.pg import PgConnect


def generate_hash_key(*values: object) -> uuid.UUID:
    # Генерируем детерминированный hash key из переданных значений
    source = '|'.join(str(value) for value in values)

    return uuid.uuid5(
        uuid.NAMESPACE_DNS,
        source
    )


class DdsRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def save_order(self, message: dict) -> None:
        # Одно Kafka-сообщение соответствует одному заказу
        payload = message['payload']

        user = payload['user']
        restaurant = payload['restaurant']
        products = payload['products']

        # Для всех сущностей одного заказа используем одно время загрузки
        load_dt = datetime.utcnow()

        # Источник загрузки данных в DDS
        load_src = 'stg-service-kafka'

        order_id = payload['id']
        order_dt = datetime.fromisoformat(payload['date'])

        # Генерируем hash key пользователя
        h_user_pk = generate_hash_key(
            'user',
            user['id']
        )

        # Генерируем hash key ресторана
        h_restaurant_pk = generate_hash_key(
            'restaurant',
            restaurant['id']
        )

        # Генерируем hash key заказа
        h_order_pk = generate_hash_key(
            'order',
            order_id
        )

        # Весь заказ записываем в одной транзакции
        with self._db.connection() as conn:
            with conn.cursor() as cur:

                # Записываем Hub пользователя
                self._insert_h_user(
                    cur,
                    h_user_pk,
                    user['id'],
                    load_dt,
                    load_src
                )

                # Записываем Hub ресторана
                self._insert_h_restaurant(
                    cur,
                    h_restaurant_pk,
                    restaurant['id'],
                    load_dt,
                    load_src
                )

                # Записываем Hub заказа
                self._insert_h_order(
                    cur,
                    h_order_pk,
                    order_id,
                    order_dt,
                    load_dt,
                    load_src
                )

                # Записываем Satellite пользователя
                self._insert_s_user_names(
                    cur,
                    h_user_pk,
                    user['name'],
                    user['login'],
                    load_dt,
                    load_src
                )

                # Записываем Satellite ресторана
                self._insert_s_restaurant_names(
                    cur,
                    h_restaurant_pk,
                    restaurant['name'],
                    load_dt,
                    load_src
                )

                # Записываем Satellite стоимости заказа
                self._insert_s_order_cost(
                    cur,
                    h_order_pk,
                    payload['cost'],
                    payload['payment'],
                    load_dt,
                    load_src
                )

                # Записываем Satellite статуса заказа
                self._insert_s_order_status(
                    cur,
                    h_order_pk,
                    payload['status'],
                    load_dt,
                    load_src
                )

                # Генерируем hash key связи заказа с пользователем
                hk_order_user_pk = generate_hash_key(
                    'order_user',
                    h_order_pk,
                    h_user_pk
                )

                # Записываем связь заказа с пользователем
                self._insert_l_order_user(
                    cur,
                    hk_order_user_pk,
                    h_order_pk,
                    h_user_pk,
                    load_dt,
                    load_src
                )

                # Обрабатываем все товары заказа
                for product in products:

                    # Генерируем hash key товара
                    h_product_pk = generate_hash_key(
                        'product',
                        product['id']
                    )

                    # Генерируем hash key категории
                    h_category_pk = generate_hash_key(
                        'category',
                        product['category']
                    )

                    # Записываем Hub товара
                    self._insert_h_product(
                        cur,
                        h_product_pk,
                        product['id'],
                        load_dt,
                        load_src
                    )

                    # Записываем Hub категории
                    self._insert_h_category(
                        cur,
                        h_category_pk,
                        product['category'],
                        load_dt,
                        load_src
                    )

                    # Записываем Satellite товара
                    self._insert_s_product_names(
                        cur,
                        h_product_pk,
                        product['name'],
                        load_dt,
                        load_src
                    )

                    # Генерируем hash key связи заказа с товаром
                    hk_order_product_pk = generate_hash_key(
                        'order_product',
                        h_order_pk,
                        h_product_pk
                    )

                    # Записываем связь заказа с товаром
                    self._insert_l_order_product(
                        cur,
                        hk_order_product_pk,
                        h_order_pk,
                        h_product_pk,
                        load_dt,
                        load_src
                    )

                    # Генерируем hash key связи товара с рестораном
                    hk_product_restaurant_pk = generate_hash_key(
                        'product_restaurant',
                        h_product_pk,
                        h_restaurant_pk
                    )

                    # Записываем связь товара с рестораном
                    self._insert_l_product_restaurant(
                        cur,
                        hk_product_restaurant_pk,
                        h_product_pk,
                        h_restaurant_pk,
                        load_dt,
                        load_src
                    )

                    # Генерируем hash key связи товара с категорией
                    hk_product_category_pk = generate_hash_key(
                        'product_category',
                        h_product_pk,
                        h_category_pk
                    )

                    # Записываем связь товара с категорией
                    self._insert_l_product_category(
                        cur,
                        hk_product_category_pk,
                        h_product_pk,
                        h_category_pk,
                        load_dt,
                        load_src
                    )

    def _insert_h_user(
        self,
        cur,
        h_user_pk: uuid.UUID,
        user_id: str,
        load_dt: datetime,
        load_src: str
    ) -> None:
        # Записываем пользователя, если такого hash key ещё нет
        cur.execute(
            """
            INSERT INTO dds.h_user
            (
                h_user_pk,
                user_id,
                load_dt,
                load_src
            )
            VALUES
            (
                %(h_user_pk)s,
                %(user_id)s,
                %(load_dt)s,
                %(load_src)s
            )
            ON CONFLICT (h_user_pk) DO NOTHING
            """,
            {
                'h_user_pk': h_user_pk,
                'user_id': user_id,
                'load_dt': load_dt,
                'load_src': load_src
            }
        )

    def _insert_h_product(
        self,
        cur,
        h_product_pk: uuid.UUID,
        product_id: str,
        load_dt: datetime,
        load_src: str
    ) -> None:
        # Записываем товар, если такого hash key ещё нет
        cur.execute(
            """
            INSERT INTO dds.h_product
            (
                h_product_pk,
                product_id,
                load_dt,
                load_src
            )
            VALUES
            (
                %(h_product_pk)s,
                %(product_id)s,
                %(load_dt)s,
                %(load_src)s
            )
            ON CONFLICT (h_product_pk) DO NOTHING
            """,
            {
                'h_product_pk': h_product_pk,
                'product_id': product_id,
                'load_dt': load_dt,
                'load_src': load_src
            }
        )

    def _insert_h_category(
        self,
        cur,
        h_category_pk: uuid.UUID,
        category_name: str,
        load_dt: datetime,
        load_src: str
    ) -> None:
        # Записываем категорию, если такого hash key ещё нет
        cur.execute(
            """
            INSERT INTO dds.h_category
            (
                h_category_pk,
                category_name,
                load_dt,
                load_src
            )
            VALUES
            (
                %(h_category_pk)s,
                %(category_name)s,
                %(load_dt)s,
                %(load_src)s
            )
            ON CONFLICT (h_category_pk) DO NOTHING
            """,
            {
                'h_category_pk': h_category_pk,
                'category_name': category_name,
                'load_dt': load_dt,
                'load_src': load_src
            }
        )

    def _insert_h_restaurant(
        self,
        cur,
        h_restaurant_pk: uuid.UUID,
        restaurant_id: str,
        load_dt: datetime,
        load_src: str
    ) -> None:
        # Записываем ресторан, если такого hash key ещё нет
        cur.execute(
            """
            INSERT INTO dds.h_restaurant
            (
                h_restaurant_pk,
                restaurant_id,
                load_dt,
                load_src
            )
            VALUES
            (
                %(h_restaurant_pk)s,
                %(restaurant_id)s,
                %(load_dt)s,
                %(load_src)s
            )
            ON CONFLICT (h_restaurant_pk) DO NOTHING
            """,
            {
                'h_restaurant_pk': h_restaurant_pk,
                'restaurant_id': restaurant_id,
                'load_dt': load_dt,
                'load_src': load_src
            }
        )

    def _insert_h_order(
        self,
        cur,
        h_order_pk: uuid.UUID,
        order_id: int,
        order_dt: datetime,
        load_dt: datetime,
        load_src: str
    ) -> None:
        # Записываем заказ, если такого hash key ещё нет
        cur.execute(
            """
            INSERT INTO dds.h_order
            (
                h_order_pk,
                order_id,
                order_dt,
                load_dt,
                load_src
            )
            VALUES
            (
                %(h_order_pk)s,
                %(order_id)s,
                %(order_dt)s,
                %(load_dt)s,
                %(load_src)s
            )
            ON CONFLICT (h_order_pk) DO NOTHING
            """,
            {
                'h_order_pk': h_order_pk,
                'order_id': order_id,
                'order_dt': order_dt,
                'load_dt': load_dt,
                'load_src': load_src
            }
        )

    def _insert_l_order_user(
        self,
        cur,
        hk_order_user_pk: uuid.UUID,
        h_order_pk: uuid.UUID,
        h_user_pk: uuid.UUID,
        load_dt: datetime,
        load_src: str
    ) -> None:
        # Записываем связь заказа с пользователем
        cur.execute(
            """
            INSERT INTO dds.l_order_user
            (
                hk_order_user_pk,
                h_order_pk,
                h_user_pk,
                load_dt,
                load_src
            )
            VALUES
            (
                %(hk_order_user_pk)s,
                %(h_order_pk)s,
                %(h_user_pk)s,
                %(load_dt)s,
                %(load_src)s
            )
            ON CONFLICT (hk_order_user_pk) DO NOTHING
            """,
            {
                'hk_order_user_pk': hk_order_user_pk,
                'h_order_pk': h_order_pk,
                'h_user_pk': h_user_pk,
                'load_dt': load_dt,
                'load_src': load_src
            }
        )

    def _insert_l_order_product(
        self,
        cur,
        hk_order_product_pk: uuid.UUID,
        h_order_pk: uuid.UUID,
        h_product_pk: uuid.UUID,
        load_dt: datetime,
        load_src: str
    ) -> None:
        # Записываем связь заказа с товаром
        cur.execute(
            """
            INSERT INTO dds.l_order_product
            (
                hk_order_product_pk,
                h_order_pk,
                h_product_pk,
                load_dt,
                load_src
            )
            VALUES
            (
                %(hk_order_product_pk)s,
                %(h_order_pk)s,
                %(h_product_pk)s,
                %(load_dt)s,
                %(load_src)s
            )
            ON CONFLICT (hk_order_product_pk) DO NOTHING
            """,
            {
                'hk_order_product_pk': hk_order_product_pk,
                'h_order_pk': h_order_pk,
                'h_product_pk': h_product_pk,
                'load_dt': load_dt,
                'load_src': load_src
            }
        )

    def _insert_l_product_restaurant(
        self,
        cur,
        hk_product_restaurant_pk: uuid.UUID,
        h_product_pk: uuid.UUID,
        h_restaurant_pk: uuid.UUID,
        load_dt: datetime,
        load_src: str
    ) -> None:
        # Записываем связь товара с рестораном
        cur.execute(
            """
            INSERT INTO dds.l_product_restaurant
            (
                hk_product_restaurant_pk,
                h_product_pk,
                h_restaurant_pk,
                load_dt,
                load_src
            )
            VALUES
            (
                %(hk_product_restaurant_pk)s,
                %(h_product_pk)s,
                %(h_restaurant_pk)s,
                %(load_dt)s,
                %(load_src)s
            )
            ON CONFLICT (hk_product_restaurant_pk) DO NOTHING
            """,
            {
                'hk_product_restaurant_pk': hk_product_restaurant_pk,
                'h_product_pk': h_product_pk,
                'h_restaurant_pk': h_restaurant_pk,
                'load_dt': load_dt,
                'load_src': load_src
            }
        )

    def _insert_l_product_category(
        self,
        cur,
        hk_product_category_pk: uuid.UUID,
        h_product_pk: uuid.UUID,
        h_category_pk: uuid.UUID,
        load_dt: datetime,
        load_src: str
    ) -> None:
        # Записываем связь товара с категорией
        cur.execute(
            """
            INSERT INTO dds.l_product_category
            (
                hk_product_category_pk,
                h_product_pk,
                h_category_pk,
                load_dt,
                load_src
            )
            VALUES
            (
                %(hk_product_category_pk)s,
                %(h_product_pk)s,
                %(h_category_pk)s,
                %(load_dt)s,
                %(load_src)s
            )
            ON CONFLICT (hk_product_category_pk) DO NOTHING
            """,
            {
                'hk_product_category_pk': hk_product_category_pk,
                'h_product_pk': h_product_pk,
                'h_category_pk': h_category_pk,
                'load_dt': load_dt,
                'load_src': load_src
            }
        )

    def _insert_s_user_names(
        self,
        cur,
        h_user_pk: uuid.UUID,
        username: str,
        userlogin: str,
        load_dt: datetime,
        load_src: str
    ) -> None:
        # Считаем hashdiff по описательным атрибутам пользователя
        hashdiff = generate_hash_key(
            'user_names',
            username,
            userlogin
        )

        # Записываем новую версию только если данные изменились
        cur.execute(
            """
            INSERT INTO dds.s_user_names
            (
                h_user_pk,
                username,
                userlogin,
                load_dt,
                load_src,
                hk_user_names_hashdiff
            )
            SELECT
                %(h_user_pk)s,
                %(username)s,
                %(userlogin)s,
                %(load_dt)s,
                %(load_src)s,
                %(hashdiff)s
            WHERE NOT EXISTS
            (
                SELECT 1
                FROM
                (
                    SELECT hk_user_names_hashdiff
                    FROM dds.s_user_names
                    WHERE h_user_pk = %(h_user_pk)s
                    ORDER BY load_dt DESC
                    LIMIT 1
                ) latest
                WHERE latest.hk_user_names_hashdiff = %(hashdiff)s
            )
            """,
            {
                'h_user_pk': h_user_pk,
                'username': username,
                'userlogin': userlogin,
                'load_dt': load_dt,
                'load_src': load_src,
                'hashdiff': hashdiff
            }
        )

    def _insert_s_product_names(
        self,
        cur,
        h_product_pk: uuid.UUID,
        name: str,
        load_dt: datetime,
        load_src: str
    ) -> None:
        # Считаем hashdiff по названию товара
        hashdiff = generate_hash_key(
            'product_names',
            name
        )

        # Записываем новую версию только если название изменилось
        cur.execute(
            """
            INSERT INTO dds.s_product_names
            (
                h_product_pk,
                name,
                load_dt,
                load_src,
                hk_product_names_hashdiff
            )
            SELECT
                %(h_product_pk)s,
                %(name)s,
                %(load_dt)s,
                %(load_src)s,
                %(hashdiff)s
            WHERE NOT EXISTS
            (
                SELECT 1
                FROM
                (
                    SELECT hk_product_names_hashdiff
                    FROM dds.s_product_names
                    WHERE h_product_pk = %(h_product_pk)s
                    ORDER BY load_dt DESC
                    LIMIT 1
                ) latest
                WHERE latest.hk_product_names_hashdiff = %(hashdiff)s
            )
            """,
            {
                'h_product_pk': h_product_pk,
                'name': name,
                'load_dt': load_dt,
                'load_src': load_src,
                'hashdiff': hashdiff
            }
        )

    def _insert_s_restaurant_names(
        self,
        cur,
        h_restaurant_pk: uuid.UUID,
        name: str,
        load_dt: datetime,
        load_src: str
    ) -> None:
        # Считаем hashdiff по названию ресторана
        hashdiff = generate_hash_key(
            'restaurant_names',
            name
        )

        # Записываем новую версию только если название изменилось
        cur.execute(
            """
            INSERT INTO dds.s_restaurant_names
            (
                h_restaurant_pk,
                name,
                load_dt,
                load_src,
                hk_restaurant_names_hashdiff
            )
            SELECT
                %(h_restaurant_pk)s,
                %(name)s,
                %(load_dt)s,
                %(load_src)s,
                %(hashdiff)s
            WHERE NOT EXISTS
            (
                SELECT 1
                FROM
                (
                    SELECT hk_restaurant_names_hashdiff
                    FROM dds.s_restaurant_names
                    WHERE h_restaurant_pk = %(h_restaurant_pk)s
                    ORDER BY load_dt DESC
                    LIMIT 1
                ) latest
                WHERE latest.hk_restaurant_names_hashdiff = %(hashdiff)s
            )
            """,
            {
                'h_restaurant_pk': h_restaurant_pk,
                'name': name,
                'load_dt': load_dt,
                'load_src': load_src,
                'hashdiff': hashdiff
            }
        )

    def _insert_s_order_cost(
        self,
        cur,
        h_order_pk: uuid.UUID,
        cost,
        payment,
        load_dt: datetime,
        load_src: str
    ) -> None:
        # Считаем hashdiff по стоимости и оплате заказа
        hashdiff = generate_hash_key(
            'order_cost',
            cost,
            payment
        )

        # Записываем новую версию только если стоимость или оплата изменились
        cur.execute(
            """
            INSERT INTO dds.s_order_cost
            (
                h_order_pk,
                cost,
                payment,
                load_dt,
                load_src,
                hk_order_cost_hashdiff
            )
            SELECT
                %(h_order_pk)s,
                %(cost)s,
                %(payment)s,
                %(load_dt)s,
                %(load_src)s,
                %(hashdiff)s
            WHERE NOT EXISTS
            (
                SELECT 1
                FROM
                (
                    SELECT hk_order_cost_hashdiff
                    FROM dds.s_order_cost
                    WHERE h_order_pk = %(h_order_pk)s
                    ORDER BY load_dt DESC
                    LIMIT 1
                ) latest
                WHERE latest.hk_order_cost_hashdiff = %(hashdiff)s
            )
            """,
            {
                'h_order_pk': h_order_pk,
                'cost': cost,
                'payment': payment,
                'load_dt': load_dt,
                'load_src': load_src,
                'hashdiff': hashdiff
            }
        )

    def _insert_s_order_status(
        self,
        cur,
        h_order_pk: uuid.UUID,
        status: str,
        load_dt: datetime,
        load_src: str
    ) -> None:
        # Считаем hashdiff по статусу заказа
        hashdiff = generate_hash_key(
            'order_status',
            status
        )

        # Записываем новую версию только если статус изменился
        cur.execute(
            """
            INSERT INTO dds.s_order_status
            (
                h_order_pk,
                status,
                load_dt,
                load_src,
                hk_order_status_hashdiff
            )
            SELECT
                %(h_order_pk)s,
                %(status)s,
                %(load_dt)s,
                %(load_src)s,
                %(hashdiff)s
            WHERE NOT EXISTS
            (
                SELECT 1
                FROM
                (
                    SELECT hk_order_status_hashdiff
                    FROM dds.s_order_status
                    WHERE h_order_pk = %(h_order_pk)s
                    ORDER BY load_dt DESC
                    LIMIT 1
                ) latest
                WHERE latest.hk_order_status_hashdiff = %(hashdiff)s
            )
            """,
            {
                'h_order_pk': h_order_pk,
                'status': status,
                'load_dt': load_dt,
                'load_src': load_src,
                'hashdiff': hashdiff
            }
        )