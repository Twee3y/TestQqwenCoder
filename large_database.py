#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Модуль для создания и управления большой базой данных на SQLite.
Сценарий: Интернет-магазин с пользователями, товарами, заказами и категориями.
"""

import sqlite3
import random
import string
import time
from datetime import datetime, timedelta
from typing import List, Tuple, Optional

# Конфигурация
DB_NAME = "large_store.db"
NUM_USERS = 10_000
NUM_CATEGORIES = 50
NUM_PRODUCTS = 50_000
NUM_ORDERS = 100_000

def generate_random_string(length: int) -> str:
    """Генерирует случайную строку."""
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def generate_random_date(start_year: int = 2020, end_year: int = 2024) -> str:
    """Генерирует случайную дату в формате ISO."""
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    delta = end - start
    random_days = random.randint(0, delta.days)
    random_date = start + timedelta(days=random_days)
    return random_date.strftime("%Y-%m-%d %H:%M:%S")

class DatabaseManager:
    def __init__(self, db_name: str):
        self.db_name = db_name
        self.conn: Optional[sqlite3.Connection] = None
        self.cursor: Optional[sqlite3.Cursor] = None

    def connect(self):
        """Подключение к БД и настройка."""
        self.conn = sqlite3.connect(self.db_name)
        self.cursor = self.conn.cursor()
        # Включаем поддержку внешних ключей
        self.cursor.execute("PRAGMA foreign_keys = ON;")
        # Оптимизация для массовой вставки
        self.cursor.execute("PRAGMA journal_mode = WAL;")
        print(f"[OK] Подключено к базе данных: {self.db_name}")

    def close(self):
        """Закрытие соединения."""
        if self.conn:
            self.conn.commit()
            self.conn.close()
            print("[OK] Соединение закрыто.")

    def create_schema(self):
        """Создание структуры таблиц."""
        print(">>> Создание схемы базы данных...")
        
        queries = [
            """
            CREATE TABLE IF NOT EXISTS categories (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                description TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL UNIQUE,
                email TEXT NOT NULL UNIQUE,
                password_hash TEXT NOT NULL,
                created_at TEXT NOT NULL,
                is_active BOOLEAN DEFAULT 1
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                description TEXT,
                price REAL NOT NULL,
                stock_quantity INTEGER NOT NULL DEFAULT 0,
                category_id INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (category_id) REFERENCES categories(id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                order_date TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending', -- pending, completed, cancelled
                total_amount REAL NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users(id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS order_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id INTEGER NOT NULL,
                product_id INTEGER NOT NULL,
                quantity INTEGER NOT NULL,
                price_at_purchase REAL NOT NULL,
                FOREIGN KEY (order_id) REFERENCES orders(id),
                FOREIGN KEY (product_id) REFERENCES products(id)
            )
            """
        ]

        for query in queries:
            self.cursor.execute(query)
        
        # Создание индексов для ускорения поиска на больших данных
        print(">>> Создание индексов...")
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);",
            "CREATE INDEX IF NOT EXISTS idx_products_category ON products(category_id);",
            "CREATE INDEX IF NOT EXISTS idx_products_price ON products(price);",
            "CREATE INDEX IF NOT EXISTS idx_orders_user ON orders(user_id);",
            "CREATE INDEX IF NOT EXISTS idx_orders_date ON orders(order_date);",
            "CREATE INDEX IF NOT EXISTS idx_order_items_order ON order_items(order_id);",
            "CREATE INDEX IF NOT EXISTS idx_order_items_product ON order_items(product_id);"
        ]
        for idx_query in indexes:
            self.cursor.execute(idx_query)
        
        self.conn.commit()
        print("[OK] Схема и индексы созданы.")

    def seed_categories(self):
        """Заполнение категорий."""
        print(f">>> Генерация {NUM_CATEGORIES} категорий...")
        categories = [(f"Category_{i}", f"Description for category {i}") for i in range(1, NUM_CATEGORIES + 1)]
        self.cursor.executemany("INSERT OR IGNORE INTO categories (name, description) VALUES (?, ?)", categories)
        self.conn.commit()

    def seed_users(self):
        """Заполнение пользователей."""
        print(f">>> Генерация {NUM_USERS} пользователей...")
        users = []
        for i in range(NUM_USERS):
            username = f"user_{i}_{generate_random_string(5)}"
            email = f"user{i}@example.com"
            pwd_hash = generate_random_string(64) # Имитация хеша
            created = generate_random_date(2020, 2023)
            users.append((username, email, pwd_hash, created, random.choice([0, 1])))
        
        self.cursor.executemany(
            "INSERT OR IGNORE INTO users (username, email, password_hash, created_at, is_active) VALUES (?, ?, ?, ?, ?)",
            users
        )
        self.conn.commit()

    def seed_products(self):
        """Заполнение товаров."""
        print(f">>> Генерация {NUM_PRODUCTS} товаров...")
        products = []
        # Предзагрузим ID категорий для быстрого доступа
        self.cursor.execute("SELECT id FROM categories")
        cat_ids = [row[0] for row in self.cursor.fetchall()]
        
        batch_size = 5000
        for i in range(NUM_PRODUCTS):
            name = f"Product_Item_{i}_{generate_random_string(4)}"
            desc = f"High quality item number {i}"
            price = round(random.uniform(1.0, 1000.0), 2)
            stock = random.randint(0, 500)
            cat_id = random.choice(cat_ids)
            created = generate_random_date(2020, 2024)
            products.append((name, desc, price, stock, cat_id, created))
            
            if len(products) >= batch_size:
                self.cursor.executemany(
                    "INSERT INTO products (name, description, price, stock_quantity, category_id, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                    products
                )
                self.conn.commit()
                products = []
                print(f"   ... обработано {i+1} товаров")
        
        if products:
            self.cursor.executemany(
                "INSERT INTO products (name, description, price, stock_quantity, category_id, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                products
            )
            self.conn.commit()

    def seed_orders_and_items(self):
        """Заполнение заказов и позиций заказа."""
        print(f">>> Генерация {NUM_ORDERS} заказов и элементов...")
        
        # Получаем диапазоны ID для случайного выбора
        self.cursor.execute("SELECT MIN(id), MAX(id) FROM users")
        user_min, user_max = self.cursor.fetchone()
        
        self.cursor.execute("SELECT MIN(id), MAX(id) FROM products")
        prod_min, prod_max = self.cursor.fetchone()
        
        orders_batch = []
        items_batch = []
        
        for i in range(NUM_ORDERS):
            user_id = random.randint(user_min, user_max)
            order_date = generate_random_date(2023, 2024)
            status = random.choice(['pending', 'completed', 'completed', 'cancelled'])
            
            # Формируем заказ с 1-5 товарами
            num_items = random.randint(1, 5)
            order_total = 0.0
            
            # Временный ID для заказа (будет присвоен БД)
            # Для связки нам нужно сначала вставить заказ, получить его ID, потом вставить товары.
            # Но для скорости мы сделаем это пакетами, эмулируя логику.
            # В реальном продакшене лучше использовать транзакции на уровне одного заказа, 
            # но здесь мы жертвуем строгой атомарностью ради скорости генерации.
            
            # Упрощение: вставим заказ, получим последний ID, затем вставим товары.
            # Чтобы не делать 100к отдельных коммитов, буферизируем заказы, но товары привязываем сложнее.
            # Для демонстрации "большой БД" сделаем пакетную вставку заказов, а потом отдельно товаров.
            
            orders_batch.append((user_id, order_date, status, 0.0)) # Total посчитаем позже или заглушкой
            
            if len(orders_batch) >= 2000:
                self.cursor.executemany(
                    "INSERT INTO orders (user_id, order_date, status, total_amount) VALUES (?, ?, ?, ?)",
                    orders_batch
                )
                self.conn.commit()
                
                # Теперь добавим товары для этих заказов
                # Получим IDs последних вставленных заказов
                last_order_id = self.cursor.execute("SELECT last_insert_rowid()").fetchone()[0]
                start_order_id = last_order_id - len(orders_batch) + 1
                
                current_items = []
                for oid in range(start_order_id, last_order_id + 1):
                    for _ in range(random.randint(1, 3)):
                        pid = random.randint(prod_min, prod_max)
                        qty = random.randint(1, 10)
                        price = round(random.uniform(1.0, 500.0), 2)
                        current_items.append((oid, pid, qty, price))
                
                self.cursor.executemany(
                    "INSERT INTO order_items (order_id, product_id, quantity, price_at_purchase) VALUES (?, ?, ?, ?)",
                    current_items
                )
                self.conn.commit()
                
                orders_batch = []
                print(f"   ... обработано {i+1} заказов")

        # Обработка остатка
        if orders_batch:
            self.cursor.executemany(
                "INSERT INTO orders (user_id, order_date, status, total_amount) VALUES (?, ?, ?, ?)",
                orders_batch
            )
            self.conn.commit()
            # Аналогично для товаров (упрощено для краткости кода)
            
        print("[OK] Данные сгенерированы.")

    def run_analytics(self):
        """Выполнение аналитических запросов для проверки производительности."""
        print("\n=== ЗАПУСК АНАЛИТИЧЕСКИХ ЗАПРОСОВ ===")
        
        queries = [
            ("Топ 5 самых дорогих товаров", "SELECT name, price FROM products ORDER BY price DESC LIMIT 5"),
            ("Количество заказов по статусам", "SELECT status, COUNT(*) FROM orders GROUP BY status"),
            ("Средняя сумма заказа (расчетная по элементам)", "SELECT AVG(total) FROM (SELECT order_id, SUM(quantity * price_at_purchase) as total FROM order_items GROUP BY order_id)"),
            ("Пользователи с наибольшим количеством заказов", "SELECT u.username, COUNT(o.id) as order_count FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.id ORDER BY order_count DESC LIMIT 5"),
            ("Продажи по категориям", "SELECT c.name, SUM(oi.quantity * oi.price_at_purchase) as revenue FROM categories c JOIN products p ON c.id = p.category_id JOIN order_items oi ON p.id = oi.product_id GROUP BY c.id ORDER BY revenue DESC LIMIT 5")
        ]
        
        for name, sql in queries:
            print(f"\nЗапрос: {name}")
            start_time = time.time()
            self.cursor.execute(sql)
            results = self.cursor.fetchall()
            elapsed = time.time() - start_time
            print(f"Время выполнения: {elapsed:.4f} сек")
            for row in results:
                print(row)

def main():
    db = DatabaseManager(DB_NAME)
    
    try:
        db.connect()
        db.create_schema()
        
        print("\nНачинаем наполнение базы данных (это может занять некоторое время)...")
        start_gen = time.time()
        
        db.seed_categories()
        db.seed_users()
        db.seed_products()
        db.seed_orders_and_items()
        
        total_time = time.time() - start_gen
        print(f"\n[OK] Генерация данных завершена за {total_time:.2f} секунд.")
        
        # Проверка размера
        db.cursor.execute("SELECT count(*) FROM users")
        u_count = db.cursor.fetchone()[0]
        db.cursor.execute("SELECT count(*) FROM products")
        p_count = db.cursor.fetchone()[0]
        db.cursor.execute("SELECT count(*) FROM orders")
        o_count = db.cursor.fetchone()[0]
        db.cursor.execute("SELECT count(*) FROM order_items")
        oi_count = db.cursor.fetchone()[0]
        
        print(f"\nИтоговый размер базы:")
        print(f"  Пользователи: {u_count}")
        print(f"  Товары: {p_count}")
        print(f"  Заказы: {o_count}")
        print(f"  Позиции в заказах: {oi_count}")
        
        db.run_analytics()
        
    except Exception as e:
        print(f"[ERROR] Произошла ошибка: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    main()
