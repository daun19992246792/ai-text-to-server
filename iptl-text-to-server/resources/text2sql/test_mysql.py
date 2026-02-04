from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import random
import string

from sql_executor import SQLExecutor
from sql_vaildator import SQLSecurityChecker

DB_CONFIG = {
    "user": "root",
    "password": "dky20030224",
    "host": "localhost",
    "port": 3306,
    "db": "test_security_db"
}
DATABASE_URL = f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['db']}"


def random_string(length=10):
    return ''.join(random.choices(string.ascii_letters, k=length))


def random_email():
    return f"{random_string(8)}@{random.choice(['gmail.com', 'yahoo.com', 'company.com'])}"


def setup_test_data():
    """初始化测试表和大量数据"""
    temp_engine = create_engine(
        f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}")
    with temp_engine.connect() as conn:
        conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {DB_CONFIG['db']}"))

    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        print("📦 开始创建表结构并插入大量数据...\n")

        # 1. 产品表 - 10000条
        print("1️⃣  创建 products 表...")
        conn.execute(text("DROP TABLE IF EXISTS products"))
        conn.execute(text("""
            CREATE TABLE products (
                id INT PRIMARY KEY AUTO_INCREMENT,
                name VARCHAR(200), category VARCHAR(50), subcategory VARCHAR(50),
                brand VARCHAR(50), price DECIMAL(10,2), cost DECIMAL(10,2),
                stock INT, warehouse VARCHAR(50), supplier_id INT,
                rating DECIMAL(3,2), reviews INT, is_active BOOLEAN,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))

        categories = ['Electronics', 'Accessories', 'Furniture', 'Clothing', 'Sports', 'Books', 'Toys', 'Food']
        brands = ['BrandA', 'BrandB', 'BrandC', 'BrandD', 'BrandE']
        warehouses = ['WH-North', 'WH-South', 'WH-East', 'WH-West']

        for batch in range(10):
            values = []
            for i in range(1000):
                cat = random.choice(categories)
                name = f"{random.choice(brands)} {cat} {random_string(6)}"
                price = round(random.uniform(10, 2000), 2)
                cost = round(price * 0.6, 2)
                stock = random.randint(0, 1000)
                values.append(
                    f"('{name}','{cat}','{cat}-Sub','{random.choice(brands)}',{price},{cost},{stock},'{random.choice(warehouses)}',{random.randint(1, 50)},{round(random.uniform(1, 5), 2)},{random.randint(0, 5000)},{random.choice([1, 0])})")

            conn.execute(text(
                f"INSERT INTO products (name,category,subcategory,brand,price,cost,stock,warehouse,supplier_id,rating,reviews,is_active) VALUES {','.join(values)}"))
            print(f"   ✓ 已插入 {(batch + 1) * 1000} / 10000 条产品")

        # 2. 订单表 - 50000条
        print("\n2️⃣  创建 orders 表...")
        conn.execute(text("DROP TABLE IF EXISTS orders"))
        conn.execute(text("""
            CREATE TABLE orders (
                id INT PRIMARY KEY AUTO_INCREMENT,
                order_number VARCHAR(50), product_id INT, customer_id INT,
                customer_name VARCHAR(100), customer_email VARCHAR(100),
                quantity INT, unit_price DECIMAL(10,2), total_amount DECIMAL(10,2),
                discount DECIMAL(10,2), tax DECIMAL(10,2),
                shipping_address VARCHAR(200), order_status VARCHAR(20),
                payment_method VARCHAR(20), order_date DATE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))

        statuses = ['pending', 'processing', 'shipped', 'delivered', 'cancelled']
        payments = ['credit_card', 'debit_card', 'paypal', 'bitcoin']

        for batch in range(50):
            values = []
            for i in range(1000):
                idx = batch * 1000 + i + 1
                order_num = f"ORD-2024-{idx:08d}"
                prod_id = random.randint(1, 10000)
                cust_id = random.randint(1, 20000)
                qty = random.randint(1, 10)
                unit_price = round(random.uniform(10, 500), 2)
                total = round(qty * unit_price, 2)
                discount = round(total * random.uniform(0, 0.2), 2)
                tax = round(total * 0.1, 2)
                order_date = (datetime.now() - timedelta(days=random.randint(0, 365))).strftime('%Y-%m-%d')

                values.append(
                    f"('{order_num}',{prod_id},{cust_id},'{random_string(8)}','{random_email()}',{qty},{unit_price},{total},{discount},{tax},'Address {random.randint(1, 999)}','{random.choice(statuses)}','{random.choice(payments)}','{order_date}')")

            conn.execute(text(
                f"INSERT INTO orders (order_number,product_id,customer_id,customer_name,customer_email,quantity,unit_price,total_amount,discount,tax,shipping_address,order_status,payment_method,order_date) VALUES {','.join(values)}"))
            print(f"   ✓ 已插入 {(batch + 1) * 1000} / 50000 条订单")

        # 3. 客户表 - 20000条
        print("\n3️⃣  创建 customers 表...")
        conn.execute(text("DROP TABLE IF EXISTS customers"))
        conn.execute(text("""
            CREATE TABLE customers (
                id INT PRIMARY KEY AUTO_INCREMENT,
                first_name VARCHAR(50), last_name VARCHAR(50),
                email VARCHAR(100), phone VARCHAR(20),
                address VARCHAR(200), city VARCHAR(50), state VARCHAR(50),
                country VARCHAR(50), zip_code VARCHAR(20),
                customer_since DATE, total_orders INT, total_spent DECIMAL(12,2),
                loyalty_points INT, is_premium BOOLEAN,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))

        cities = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia', 'San Antonio',
                  'San Diego']
        states = ['NY', 'CA', 'IL', 'TX', 'AZ', 'PA']

        for batch in range(20):
            values = []
            for i in range(1000):
                fname = random_string(6)
                lname = random_string(8)
                email = f"{fname.lower()}.{lname.lower()}@{random.choice(['gmail.com', 'yahoo.com'])}"
                phone = f"+1-{random.randint(200, 999)}-{random.randint(100, 999)}-{random.randint(1000, 9999)}"
                since = (datetime.now() - timedelta(days=random.randint(0, 1095))).strftime('%Y-%m-%d')
                total_orders = random.randint(0, 100)
                total_spent = round(random.uniform(0, 50000), 2)
                points = random.randint(0, 10000)

                values.append(
                    f"('{fname}','{lname}','{email}','{phone}','Addr {random.randint(1, 999)}','{random.choice(cities)}','{random.choice(states)}','USA','{random.randint(10000, 99999)}','{since}',{total_orders},{total_spent},{points},{random.choice([1, 0])})")

            conn.execute(text(
                f"INSERT INTO customers (first_name,last_name,email,phone,address,city,state,country,zip_code,customer_since,total_orders,total_spent,loyalty_points,is_premium) VALUES {','.join(values)}"))
            print(f"   ✓ 已插入 {(batch + 1) * 1000} / 20000 条客户")

        # 4. 供应商表 - 500条
        print("\n4️⃣  创建 suppliers 表...")
        conn.execute(text("DROP TABLE IF EXISTS suppliers"))
        conn.execute(text("""
            CREATE TABLE suppliers (
                id INT PRIMARY KEY AUTO_INCREMENT,
                company_name VARCHAR(100), contact_name VARCHAR(100),
                email VARCHAR(100), phone VARCHAR(20),
                address VARCHAR(200), city VARCHAR(50), country VARCHAR(50),
                product_category VARCHAR(50), rating DECIMAL(3,2),
                total_contracts INT, is_verified BOOLEAN,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))

        values = []
        for i in range(500):
            company = f"Supplier {random_string(8)} Inc"
            contact = f"{random_string(6)} {random_string(8)}"
            cat = random.choice(categories)

            values.append(
                f"('{company}','{contact}','{random_email()}','+1-{random.randint(200, 999)}-{random.randint(100, 999)}-{random.randint(1000, 9999)}','Addr {random.randint(1, 999)}','{random.choice(cities)}','USA','{cat}',{round(random.uniform(1, 5), 2)},{random.randint(0, 500)},{random.choice([1, 0])})")

        conn.execute(text(
            f"INSERT INTO suppliers (company_name,contact_name,email,phone,address,city,country,product_category,rating,total_contracts,is_verified) VALUES {','.join(values)}"))
        print(f"   ✓ 已插入 500 条供应商")

        # 5. 库存记录表 - 30000条
        print("\n5️⃣  创建 inventory_logs 表...")
        conn.execute(text("DROP TABLE IF EXISTS inventory_logs"))
        conn.execute(text("""
            CREATE TABLE inventory_logs (
                id INT PRIMARY KEY AUTO_INCREMENT,
                product_id INT, warehouse VARCHAR(50),
                operation_type VARCHAR(20), quantity INT,
                operator VARCHAR(50), reason VARCHAR(200),
                log_date DATE, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))

        operations = ['inbound', 'outbound', 'transfer', 'adjustment', 'return']

        for batch in range(30):
            values = []
            for i in range(1000):
                prod_id = random.randint(1, 10000)
                op_type = random.choice(operations)
                qty = random.randint(-100, 500)
                log_date = (datetime.now() - timedelta(days=random.randint(0, 180))).strftime('%Y-%m-%d')

                values.append(
                    f"({prod_id},'{random.choice(warehouses)}','{op_type}',{qty},'{random_string(8)}','Reason {random.randint(1, 100)}','{log_date}')")

            conn.execute(text(
                f"INSERT INTO inventory_logs (product_id,warehouse,operation_type,quantity,operator,reason,log_date) VALUES {','.join(values)}"))
            print(f"   ✓ 已插入 {(batch + 1) * 1000} / 30000 条库存日志")

        # 6. 评论表 - 100000条
        print("\n6️⃣  创建 reviews 表...")
        conn.execute(text("DROP TABLE IF EXISTS reviews"))
        conn.execute(text("""
            CREATE TABLE reviews (
                id INT PRIMARY KEY AUTO_INCREMENT,
                product_id INT, customer_id INT,
                rating INT, title VARCHAR(200), content TEXT,
                helpful_count INT, verified_purchase BOOLEAN,
                review_date DATE, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))

        titles = ['Great product!', 'Not bad', 'Excellent quality', 'Disappointed', 'Worth the price']

        for batch in range(100):
            values = []
            for i in range(1000):
                prod_id = random.randint(1, 10000)
                cust_id = random.randint(1, 20000)
                rating = random.randint(1, 5)
                title = random.choice(titles)
                content = f"Review content {random_string(50)}"
                helpful = random.randint(0, 1000)
                review_date = (datetime.now() - timedelta(days=random.randint(0, 365))).strftime('%Y-%m-%d')

                values.append(
                    f"({prod_id},{cust_id},{rating},'{title}','{content}',{helpful},{random.choice([1, 0])},'{review_date}')")

            conn.execute(text(
                f"INSERT INTO reviews (product_id,customer_id,rating,title,content,helpful_count,verified_purchase,review_date) VALUES {','.join(values)}"))
            if (batch + 1) % 10 == 0:
                print(f"   ✓ 已插入 {(batch + 1) * 1000} / 100000 条评论")

        # 7-10. 敏感表（黑名单）
        print("\n🔒 创建敏感表（黑名单）...")

        # 7. users_secrets - 1000条
        conn.execute(text("DROP TABLE IF EXISTS users_secrets"))
        conn.execute(text("""
            CREATE TABLE users_secrets (
                id INT PRIMARY KEY AUTO_INCREMENT,
                username VARCHAR(50), password VARCHAR(255),
                email VARCHAR(100), ssn VARCHAR(20),
                credit_card VARCHAR(20), bank_account VARCHAR(30),
                api_key VARCHAR(100), security_question VARCHAR(200),
                security_answer VARCHAR(200)
            )
        """))

        values = []
        for i in range(1000):
            username = f"user_{random_string(8)}"
            password = f"hash_{random_string(32)}"
            ssn = f"{random.randint(100, 999)}-{random.randint(10, 99)}-{random.randint(1000, 9999)}"
            cc = f"{random.randint(4000, 4999)}-****-****-{random.randint(1000, 9999)}"
            bank = f"ACC-{random.randint(100000, 999999)}"
            api_key = f"sk-{random_string(40)}"

            values.append(
                f"('{username}','{password}','{random_email()}','{ssn}','{cc}','{bank}','{api_key}','Question?','Answer')")

        conn.execute(text(
            f"INSERT INTO users_secrets (username,password,email,ssn,credit_card,bank_account,api_key,security_question,security_answer) VALUES {','.join(values)}"))
        print("   ✓ 已插入 1000 条敏感用户数据")

        # 8. config_table - 100条
        conn.execute(text("DROP TABLE IF EXISTS config_table"))
        conn.execute(text("""
            CREATE TABLE config_table (
                id INT PRIMARY KEY AUTO_INCREMENT,
                config_key VARCHAR(100), config_value TEXT,
                is_sensitive BOOLEAN, environment VARCHAR(20),
                updated_by VARCHAR(50), updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))

        values = []
        configs = ['api_key', 'db_password', 'secret_token', 'encryption_key', 'oauth_secret']
        for i in range(100):
            key = f"{random.choice(configs)}_{random_string(8)}"
            value = f"secret_{random_string(32)}"
            env = random.choice(['production', 'staging', 'development'])

            values.append(f"('{key}','{value}',1,'{env}','{random_string(8)}')")

        conn.execute(text(
            f"INSERT INTO config_table (config_key,config_value,is_sensitive,environment,updated_by) VALUES {','.join(values)}"))
        print("   ✓ 已插入 100 条配置数据")

        # 9. employee_salaries - 5000条
        conn.execute(text("DROP TABLE IF EXISTS employee_salaries"))
        conn.execute(text("""
            CREATE TABLE employee_salaries (
                id INT PRIMARY KEY AUTO_INCREMENT,
                employee_id INT, first_name VARCHAR(50), last_name VARCHAR(50),
                department VARCHAR(50), position VARCHAR(100),
                base_salary DECIMAL(12,2), bonus DECIMAL(12,2),
                stock_options INT, hire_date DATE,
                performance_rating DECIMAL(3,2)
            )
        """))

        departments = ['Engineering', 'Sales', 'Marketing', 'HR', 'Finance', 'Operations']
        positions = ['Manager', 'Senior', 'Lead', 'Associate', 'Director']

        for batch in range(5):
            values = []
            for i in range(1000):
                emp_id = batch * 1000 + i + 1
                fname = random_string(6)
                lname = random_string(8)
                dept = random.choice(departments)
                pos = f"{random.choice(positions)} {dept}"
                salary = round(random.uniform(50000, 250000), 2)
                bonus = round(salary * random.uniform(0, 0.3), 2)
                stocks = random.randint(0, 50000)
                hire = (datetime.now() - timedelta(days=random.randint(0, 3650))).strftime('%Y-%m-%d')
                rating = round(random.uniform(1, 5), 2)

                values.append(
                    f"({emp_id},'{fname}','{lname}','{dept}','{pos}',{salary},{bonus},{stocks},'{hire}',{rating})")

            conn.execute(text(
                f"INSERT INTO employee_salaries (employee_id,first_name,last_name,department,position,base_salary,bonus,stock_options,hire_date,performance_rating) VALUES {','.join(values)}"))
            print(f"   ✓ 已插入 {(batch + 1) * 1000} / 5000 条员工薪资")

        # 10. audit_logs - 50000条
        conn.execute(text("DROP TABLE IF EXISTS audit_logs"))
        conn.execute(text("""
            CREATE TABLE audit_logs (
                id INT PRIMARY KEY AUTO_INCREMENT,
                user_id INT, username VARCHAR(50),
                action VARCHAR(100), table_name VARCHAR(50),
                record_id INT, ip_address VARCHAR(50),
                user_agent TEXT, success BOOLEAN,
                error_message TEXT, log_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))

        actions = ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'LOGIN', 'LOGOUT']
        tables = ['products', 'orders', 'customers', 'users_secrets']

        for batch in range(50):
            values = []
            for i in range(1000):
                user_id = random.randint(1, 1000)
                action = random.choice(actions)
                table = random.choice(tables)
                ip = f"{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}"

                values.append(
                    f"({user_id},'user_{random_string(6)}','{action}','{table}',{random.randint(1, 10000)},'{ip}','Mozilla/5.0',{random.choice([1, 0])},'Error msg')")

            conn.execute(text(
                f"INSERT INTO audit_logs (user_id,username,action,table_name,record_id,ip_address,user_agent,success,error_message) VALUES {','.join(values)}"))
            if (batch + 1) % 10 == 0:
                print(f"   ✓ 已插入 {(batch + 1) * 1000} / 50000 条审计日志")

        conn.commit()

    print("\n" + "=" * 80)
    print("✅ 测试环境初始化成功！")
    print("=" * 80)
    print("数据统计:")
    print(f"  📦 products (产品): 10,000 条")
    print(f"  📋 orders (订单): 50,000 条")
    print(f"  👥 customers (客户): 20,000 条")
    print(f"  🏭 suppliers (供应商): 500 条")
    print(f"  📊 inventory_logs (库存日志): 30,000 条")
    print(f"  ⭐ reviews (评论): 100,000 条")
    print(f"  🔒 users_secrets (黑名单): 1,000 条")
    print(f"  🔒 config_table (黑名单): 100 条")
    print(f"  🔒 employee_salaries (黑名单): 5,000 条")
    print(f"  🔒 audit_logs (黑名单): 50,000 条")
    print(f"\n  📊 总数据量: 266,600 条")
    print("=" * 80)


def run_tests():
    """运行全面的安全测试套件"""
    # 初始化检查器：拉黑敏感表，设置最大限制为 5 条
    checker = SQLSecurityChecker(
        blocked_tables={"users_secrets", "config_table", "employee_salaries", "audit_logs"},
        max_limit=5,
    )
    executor = SQLExecutor(DATABASE_URL)

    # 测试用例定义：(类别, 名称, SQL 语句, 是否预期报错, 描述)
    test_cases = [
        # ===== 1. 基础查询测试 =====
        ("基础查询", "简单SELECT",
         "SELECT * FROM products", False,
         "最基本的全表查询"),

        ("基础查询", "带列选择",
         "SELECT id, name, price FROM products", False,
         "指定列查询"),

        ("基础查询", "列别名",
         "SELECT id AS product_id, name AS product_name FROM products", False,
         "使用列别名"),

        ("基础查询", "常量表达式",
         "SELECT 'Test' as test_col, 1+1 as calc FROM products", False,
         "包含常量表达式的查询"),

        # ===== 2. WHERE条件测试 =====
        ("WHERE条件", "基础比较",
         "SELECT * FROM products WHERE price > 100 AND price < 500", False,
         "价格区间查询"),

        ("WHERE条件", "多重条件",
         "SELECT * FROM products WHERE (category = 'Electronics' OR category = 'Accessories') AND stock > 50", False,
         "复杂AND/OR组合"),

        ("WHERE条件", "NULL检查",
         "SELECT * FROM customers WHERE phone IS NULL", False,
         "IS NULL条件"),

        ("WHERE条件", "NOT条件",
         "SELECT * FROM products WHERE NOT (price < 50 OR stock = 0)", False,
         "NOT运算符"),

        ("WHERE条件", "复杂逻辑",
         "SELECT * FROM orders WHERE (order_status = 'shipped' AND total_amount > 1000) OR (payment_method = 'credit_card' AND discount > 50)",
         False,
         "多条件复杂组合"),

        # ===== 3. JOIN操作测试 =====
        ("JOIN查询", "三表JOIN",
         """SELECT p.name, o.order_number, c.first_name, c.last_name, o.total_amount
            FROM products p 
            JOIN orders o ON p.id = o.product_id 
            JOIN customers c ON o.customer_id = c.id 
            WHERE p.category = 'Electronics'""", False,
         "三表关联查询"),

        ("JOIN查询", "LEFT JOIN空值",
         """SELECT p.name, o.order_number, o.order_status 
            FROM products p 
            LEFT JOIN orders o ON p.id = o.product_id 
            WHERE o.id IS NULL""", False,
         "LEFT JOIN找未售出商品"),

        ("JOIN查询", "多条件JOIN",
         """SELECT p.name, s.company_name, p.price, s.rating 
            FROM products p 
            JOIN suppliers s ON p.supplier_id = s.id AND s.is_verified = 1 
            WHERE p.stock > 0""", False,
         "JOIN带额外条件"),

        ("JOIN查询", "自连接",
         """SELECT e1.first_name as employee, e2.first_name as manager 
            FROM employee_salaries e1 
            JOIN employee_salaries e2 ON e1.department = e2.department 
            WHERE e1.position LIKE '%Associate%' AND e2.position LIKE '%Manager%'""", False,
         "自连接查询"),

        # ===== 4. 子查询测试 =====
        ("子查询", "相关子查询",
         """SELECT p.name, p.price, 
               (SELECT AVG(price) FROM products p2 WHERE p2.category = p.category) as avg_category_price 
            FROM products p 
            WHERE p.price > (SELECT AVG(price) FROM products)""", False,
         "相关子查询计算"),

        ("子查询", "多层嵌套",
         """SELECT * FROM (
                SELECT category, AVG(price) as avg_price 
                FROM products 
                WHERE id IN (
                    SELECT product_id 
                    FROM orders 
                    WHERE order_date > '2024-01-01'
                )
                GROUP BY category
            ) t WHERE avg_price > 200""", False,
         "三层嵌套子查询"),

        ("子查询", "NOT EXISTS",
         """SELECT c.first_name, c.last_name 
            FROM customers c 
            WHERE NOT EXISTS (
                SELECT 1 
                FROM orders o 
                WHERE o.customer_id = c.id AND o.order_date > '2024-06-01'
            )""", False,
         "NOT EXISTS子查询"),

        ("子查询", "多列子查询",
         """SELECT * FROM employee_salaries 
            WHERE (department, base_salary) IN (
                SELECT department, MAX(base_salary) 
                FROM employee_salaries 
                GROUP BY department
            )""", False,
         "多列IN子查询"),

        # ===== 5. 聚合函数测试 =====
        ("聚合查询", "基础聚合",
         """SELECT category, 
                   COUNT(*) as total_products,
                   AVG(price) as avg_price,
                   MIN(price) as min_price,
                   MAX(price) as max_price,
                   SUM(stock) as total_stock
            FROM products 
            GROUP BY category 
            HAVING COUNT(*) > 100""", False,
         "多聚合函数+HAVING"),

        ("聚合查询", "ROLLUP分组",
         """SELECT category, brand, COUNT(*) as count, AVG(price) as avg_price
            FROM products 
            GROUP BY category, brand WITH ROLLUP""", False,
         "WITH ROLLUP分组汇总"),

        ("聚合查询", "窗口聚合",
         """SELECT name, price, category,
                   AVG(price) OVER (PARTITION BY category) as category_avg,
                   RANK() OVER (PARTITION BY category ORDER BY price DESC) as price_rank
            FROM products""", False,
         "窗口函数聚合"),

        ("聚合查询", "条件聚合",
         """SELECT category,
                   SUM(CASE WHEN price > 500 THEN 1 ELSE 0 END) as expensive_count,
                   SUM(CASE WHEN stock < 10 THEN 1 ELSE 0 END) as low_stock_count
            FROM products 
            GROUP BY category""", False,
         "CASE WHEN条件计数"),

        # ===== 6. 排序和分页测试 =====
        ("排序分页", "多重排序",
         """SELECT name, price, rating, reviews 
            FROM products 
            ORDER BY rating DESC, price ASC, reviews DESC 
            LIMIT 20""", False,
         "多列复合排序"),

        ("排序分页", "分页查询",
         """SELECT * FROM orders 
            ORDER BY order_date DESC 
            LIMIT 10 OFFSET 20""", False,
         "标准分页查询"),

        ("排序分页", "子查询分页",
         """SELECT * FROM (
                SELECT p.name, o.order_number, o.total_amount 
                FROM products p 
                JOIN orders o ON p.id = o.product_id 
                ORDER BY o.total_amount DESC
            ) t LIMIT 5""", False,
         "子查询内排序后分页"),

        # ===== 7. 字符串函数测试 =====
        ("字符串操作", "各种字符串函数",
         """SELECT name, 
                   UPPER(name) as upper_name,
                   LOWER(name) as lower_name,
                   LENGTH(name) as name_length,
                   CONCAT(category, ' - ', brand) as full_category,
                   SUBSTRING(name, 1, 10) as short_name,
                   REPLACE(name, 'Pro', 'Professional') as replaced_name
            FROM products 
            WHERE name LIKE '%Pro%'""", False,
         "多种字符串函数组合"),

        ("字符串操作", "正则表达式",
         """SELECT * FROM customers 
            WHERE email REGEXP '^[a-zA-Z0-9._%+-]+@gmail\.com$'""", False,
         "正则表达式匹配"),

        # ===== 8. 日期函数测试 =====
        ("日期操作", "日期函数",
         """SELECT order_number, order_date,
                   YEAR(order_date) as order_year,
                   MONTH(order_date) as order_month,
                   DAY(order_date) as order_day,
                   DATE_ADD(order_date, INTERVAL 7 DAY) as expected_delivery,
                   DATEDIFF(CURDATE(), order_date) as days_since_order
            FROM orders 
            WHERE order_date > '2024-01-01'""", False,
         "多种日期函数"),

        ("日期操作", "时间范围",
         """SELECT * FROM orders 
            WHERE order_date BETWEEN '2024-01-01' AND '2024-06-30' 
              AND HOUR(created_at) BETWEEN 9 AND 17""", False,
         "日期时间范围查询"),

        # ===== 9. CASE WHEN表达式 =====
        ("CASE表达式", "简单CASE",
         """SELECT name, price,
                   CASE 
                       WHEN price < 100 THEN 'Cheap'
                       WHEN price BETWEEN 100 AND 500 THEN 'Moderate'
                       WHEN price > 500 THEN 'Expensive'
                   END as price_category
            FROM products""", False,
         "CASE WHEN价格分类"),

        ("CASE表达式", "复杂CASE",
         """SELECT id, first_name, total_spent,
                   CASE 
                       WHEN total_orders > 50 AND total_spent > 10000 THEN 'VIP'
                       WHEN total_orders > 10 AND total_spent > 1000 THEN 'Loyal'
                       WHEN total_orders > 0 THEN 'Active'
                       ELSE 'New'
                   END as customer_level,
                   CASE WHEN is_premium = 1 THEN 'Premium' ELSE 'Standard' END as membership
            FROM customers""", False,
         "复杂CASE表达式"),

        # ===== 10. UNION测试 =====
        ("UNION操作", "基础UNION",
         """SELECT 'Product' as type, name, price FROM products WHERE price > 1000
            UNION
            SELECT 'Order' as type, order_number, total_amount FROM orders WHERE total_amount > 1000
            ORDER BY price DESC""", False,
         "不同表的UNION查询"),

        ("UNION操作", "UNION ALL",
         """SELECT category, COUNT(*) as count FROM products GROUP BY category
            UNION ALL
            SELECT 'TOTAL' as category, COUNT(*) as count FROM products""", False,
         "UNION ALL保留重复"),

        ("UNION操作", "多层UNION",
         """SELECT name, price FROM products WHERE category = 'Electronics'
            UNION
            SELECT name, price FROM products WHERE category = 'Accessories'
            UNION
            SELECT name, price FROM products WHERE brand = 'BrandA'
            ORDER BY price DESC 
            LIMIT 10""", False,
         "多个UNION组合"),

        # ===== 11. CTE递归查询 =====
        ("CTE查询", "递归CTE",
         """WITH RECURSIVE number_series AS (
                SELECT 1 as n
                UNION ALL
                SELECT n + 1 FROM number_series WHERE n < 10
            )
            SELECT n FROM number_series""", False,
         "递归CTE生成序列"),

        ("CTE查询", "多重CTE",
         """WITH 
                high_value_orders AS (
                    SELECT * FROM orders WHERE total_amount > 1000
                ),
                active_customers AS (
                    SELECT * FROM customers WHERE total_orders > 5
                )
            SELECT o.order_number, c.first_name, c.last_name, o.total_amount
            FROM high_value_orders o
            JOIN active_customers c ON o.customer_id = c.id
            ORDER BY o.total_amount DESC""", False,
         "多个CTE组合"),

        # ===== 12. 窗口函数高级用法 =====
        ("窗口函数", "排名函数",
         """SELECT name, price, category,
                   ROW_NUMBER() OVER (PARTITION BY category ORDER BY price DESC) as row_num,
                   RANK() OVER (PARTITION BY category ORDER BY price DESC) as rank_num,
                   DENSE_RANK() OVER (PARTITION BY category ORDER BY price DESC) as dense_rank_num,
                   NTILE(4) OVER (PARTITION BY category ORDER BY price DESC) as price_quartile
            FROM products""", False,
         "多种排名窗口函数"),

        ("窗口函数", "累计计算",
         """SELECT order_date, total_amount,
                   SUM(total_amount) OVER (ORDER BY order_date) as running_total,
                   AVG(total_amount) OVER (ORDER BY order_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as weekly_avg
            FROM orders 
            WHERE order_date > '2024-01-01'
            ORDER BY order_date""", False,
         "累计和移动平均"),

        # ===== 13. JSON函数测试 =====
        ("JSON操作", "JSON处理",
         """SELECT id, first_name, last_name,
                   JSON_OBJECT('name', CONCAT(first_name, ' ', last_name),
                              'email', email,
                              'orders', total_orders,
                              'spent', total_spent) as customer_json
            FROM customers 
            WHERE is_premium = 1
            LIMIT 10""", False,
         "构建JSON对象"),

        # ===== 14. 黑名单表访问测试 =====
        ("黑名单拦截", "直接访问敏感表",
         "SELECT username, password, ssn, credit_card FROM users_secrets", True,
         "直接查询敏感信息"),

        ("黑名单拦截", "JOIN敏感表",
         """SELECT p.name, u.username, u.email 
            FROM products p 
            JOIN users_secrets u ON p.id = u.id""", True,
         "JOIN连接敏感表"),

        ("黑名单拦截", "子查询访问敏感表",
         """SELECT p.name, p.price 
            FROM products p 
            WHERE p.supplier_id IN (
                SELECT id FROM employee_salaries WHERE base_salary > 100000
            )""", True,
         "子查询访问薪资表"),

        ("黑名单拦截", "UNION获取敏感数据",
         """SELECT name as data FROM products WHERE id = 1
            UNION ALL
            SELECT CONCAT(first_name, ' ', last_name) as data FROM employee_salaries""", True,
         "UNION获取员工信息"),

        ("黑名单拦截", "CTE访问敏感表",
         """WITH sensitive_data AS (
                SELECT username, email FROM users_secrets
            )
            SELECT * FROM sensitive_data""", True,
         "CTE包装敏感查询"),

        ("黑名单拦截", "视图绕过尝试",
         """CREATE VIEW temp_view AS SELECT * FROM config_table""", True,
         "创建视图访问敏感表"),

        # ===== 15. 写操作拦截测试 =====
        ("写操作拦截", "INSERT多值",
         """INSERT INTO products (name, price, stock) 
            VALUES ('Test Product 1', 99.99, 100),
                   ('Test Product 2', 149.99, 50),
                   ('Test Product 3', 199.99, 25)""", True,
         "多值INSERT插入"),

        ("写操作拦截", "INSERT SELECT",
         """INSERT INTO products_backup 
            SELECT * FROM products WHERE category = 'Electronics'""", True,
         "INSERT SELECT语句"),

        ("写操作拦截", "UPDATE带JOIN",
         """UPDATE products p 
            JOIN suppliers s ON p.supplier_id = s.id
            SET p.price = p.price * 1.1 
            WHERE s.country = 'USA'""", True,
         "UPDATE带JOIN条件"),

        ("写操作拦截", "DELETE带子查询",
         """DELETE FROM orders 
            WHERE customer_id IN (
                SELECT id FROM customers 
                WHERE customer_since < '2020-01-01' AND total_orders = 0
            )""", True,
         "DELETE带子查询"),

        ("写操作拦截", "TRUNCATE多个表",
         "TRUNCATE TABLE products, orders, customers", True,
         "批量TRUNCATE表"),

        ("写操作拦截", "DROP多个表",
         "DROP TABLE IF EXISTS products, orders, customers", True,
         "批量删除表"),

        ("写操作拦截", "ALTER修改列",
         """ALTER TABLE products 
            ADD COLUMN discount_price DECIMAL(10,2),
            DROP COLUMN warehouse,
            MODIFY COLUMN price DECIMAL(12,2)""", True,
         "ALTER多操作"),

        # ===== 16. SQL注入测试 =====
        ("SQL注入", "注释绕过",
         """SELECT * FROM products WHERE id = 1 /*注释*/ OR 1=1 -- 注释
            UNION SELECT username, password FROM users_secrets""", True,
         "注释绕过尝试"),

        ("SQL注入", "时间盲注模拟",
         """SELECT * FROM products 
            WHERE id = IF(1=1, SLEEP(5), 0)""", True,
         "时间盲注模拟"),

        ("SQL注入", "报错注入模拟",
         """SELECT * FROM products 
            WHERE id = 1 AND ExtractValue(1, CONCAT(0x7e, (SELECT @@version), 0x7e))""", True,
         "报错注入尝试"),

        ("SQL注入", "布尔盲注模拟",
         """SELECT * FROM products 
            WHERE id = 1 AND (SELECT SUBSTRING(password,1,1) FROM users_secrets WHERE id=1) = 'a'""", True,
         "布尔盲注尝试"),

        ("SQL注入", "堆叠查询",
         """SELECT * FROM products; 
            UPDATE users_secrets SET password = 'hacked' WHERE id = 1;
            SELECT * FROM config_table""", True,
         "堆叠查询攻击"),

        ("SQL注入", "编码绕过",
         """SELECT * FROM products 
            WHERE name = 'test' OR 1=1 UNION SELECT 1,2,3,4,5,6,7,8,9,10,11,12,13,14 FROM users_secrets""", True,
         "编码绕过尝试"),

        # ===== 17. 危险函数测试 =====
        ("危险函数", "系统函数",
         "SELECT @@version, @@hostname, USER(), DATABASE()", True,
         "系统信息获取"),

        ("危险函数", "文件操作",
         "SELECT * FROM products INTO OUTFILE '/tmp/products_backup.csv'", True,
         "导出文件操作"),

        ("危险函数", "系统命令",
         "SELECT sys_exec('cat /etc/passwd')", True,
         "系统命令执行"),

        ("危险函数", "加解密函数",
         "SELECT AES_ENCRYPT('secret', 'key'), MD5('password'), SHA1('data')", False,
         "加密函数使用（应允许）"),

        ("危险函数", "危险组合",
         "SELECT BENCHMARK(1000000, MD5('test')), SLEEP(1) FROM products", True,
         "性能攻击组合"),

        # ===== 18. 性能边界测试 =====
        ("性能测试", "大表笛卡尔积",
         "SELECT * FROM products, orders LIMIT 100000", True,
         "大表笛卡尔积（应拦截）"),

        ("性能测试", "递归爆炸",
         """WITH RECURSIVE cte AS (
                SELECT 1 as n
                UNION ALL
                SELECT n + 1 FROM cte WHERE n < 1000000
            )
            SELECT SUM(n) FROM cte""", True,
         "递归深度过大"),

        ("性能测试", "超大LIMIT",
         "SELECT * FROM products LIMIT 1000000 OFFSET 0", True,
         "超大结果集查询"),

        ("性能测试", "复杂正则",
         "SELECT * FROM customers WHERE email REGEXP '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$'", False,
         "复杂正则表达式"),

        # ===== 19. 权限相关测试 =====
        ("权限测试", "SHOW命令",
         "SHOW TABLES", True,
         "SHOW TABLES命令"),

        ("权限测试", "DESCRIBE命令",
         "DESCRIBE products", True,
         "DESCRIBE表结构"),

        ("权限测试", "EXPLAIN分析",
         "EXPLAIN SELECT * FROM products WHERE price > 100", True,
         "EXPLAIN查询计划"),

        ("权限测试", "PROCESSLIST查看",
         "SHOW PROCESSLIST", True,
         "查看进程列表"),

        # ===== 20. 特殊场景测试 =====
        ("特殊场景", "空查询",
         "", True,
         "空查询语句"),

        ("特殊场景", "只有注释",
         "/* This is a comment */ -- Another comment", True,
         "只有注释的查询"),

        ("特殊场景", "超长查询",
         "SELECT " + "id," * 100 + "name FROM products", True,
         "超长字段列表"),

        ("特殊场景", "嵌套括号",
         "SELECT * FROM (((products))) WHERE (((price > 100)))", False,
         "多层括号嵌套"),

        ("特殊场景", "混合大小写",
         "SeLeCt * FrOm PrOdUcTs WhErE pRiCe > 100", False,
         "混合大小写SQL"),

        ("特殊场景", "带特殊字符",
         "SELECT * FROM `products` WHERE `name` LIKE '%test\\'s product%'", False,
         "带转义字符的查询"),

        ("特殊场景", "多语句中间有空白",
         "SELECT * FROM products;  \n\n  SELECT * FROM orders", True,
         "多语句带空白"),

        # ===== 21. 业务逻辑测试 =====
        ("业务逻辑", "库存预警",
         """SELECT p.name, p.stock, p.warehouse,
                   (SELECT SUM(quantity) FROM inventory_logs WHERE product_id = p.id AND operation_type = 'outbound' AND log_date > DATE_SUB(CURDATE(), INTERVAL 7 DAY)) as weekly_sales
            FROM products p
            WHERE p.stock < 20 
               OR p.stock < (SELECT AVG(quantity) FROM orders WHERE product_id = p.id AND order_date > DATE_SUB(CURDATE(), INTERVAL 30 DAY))""",
         False,
         "库存预警查询"),

        ("业务逻辑", "客户价值分析",
         """SELECT c.first_name, c.last_name, c.customer_since,
                   COUNT(DISTINCT o.id) as order_count,
                   SUM(o.total_amount) as total_spent,
                   AVG(o.total_amount) as avg_order_value,
                   MAX(o.order_date) as last_order_date,
                   CASE 
                       WHEN SUM(o.total_amount) > 10000 THEN 'VIP'
                       WHEN SUM(o.total_amount) > 1000 THEN 'Premium'
                       ELSE 'Standard'
                   END as customer_tier
            FROM customers c
            LEFT JOIN orders o ON c.id = o.customer_id
            GROUP BY c.id
            HAVING COUNT(DISTINCT o.id) > 0
            ORDER BY total_spent DESC""", False,
         "客户价值分层分析"),

        ("业务逻辑", "供应商绩效",
         """SELECT s.company_name, s.contact_name, s.rating,
                   COUNT(DISTINCT p.id) as product_count,
                   SUM(p.stock) as total_stock,
                   AVG(p.rating) as avg_product_rating,
                   COUNT(DISTINCT r.id) as review_count
            FROM suppliers s
            LEFT JOIN products p ON s.id = p.supplier_id
            LEFT JOIN reviews r ON p.id = r.product_id
            GROUP BY s.id
            HAVING COUNT(DISTINCT p.id) > 0
            ORDER BY s.rating DESC, product_count DESC""", False,
         "供应商绩效评估"),

        # ===== 22. 复杂数学运算 =====
        ("数学运算", "复杂计算",
         """SELECT name, price, cost,
                   price - cost as profit,
                   (price - cost) / price * 100 as profit_margin,
                   ROUND(price * 0.9, 2) as discounted_price,
                   POWER(price, 1.1) as adjusted_price,
                   LOG(price) as log_price,
                   SQRT(price) as sqrt_price
            FROM products 
            WHERE price > 0""", False,
         "复杂数学运算"),

        # ===== 23. 地理空间模拟测试 =====
        ("地理空间", "距离计算模拟",
         """SELECT c1.city, c2.city,
                   ABS(RAND() * 1000) as simulated_distance,
                   COUNT(DISTINCT o1.id) as orders_from_city1,
                   COUNT(DISTINCT o2.id) as orders_from_city2
            FROM customers c1
            CROSS JOIN customers c2
            LEFT JOIN orders o1 ON c1.id = o1.customer_id
            LEFT JOIN orders o2 ON c2.id = o2.customer_id
            WHERE c1.city != c2.city 
            GROUP BY c1.city, c2.city
            HAVING COUNT(DISTINCT o1.id) > 10 AND COUNT(DISTINCT o2.id) > 10
            LIMIT 10""", False,
         "模拟地理空间查询"),

        # ===== 24. 时序数据分析 =====
        ("时序分析", "销售趋势",
         """SELECT DATE_FORMAT(order_date, '%Y-%m') as month,
                   COUNT(DISTINCT o.id) as order_count,
                   SUM(o.total_amount) as total_revenue,
                   AVG(o.total_amount) as avg_order_value,
                   COUNT(DISTINCT o.customer_id) as unique_customers,
                   (SUM(o.total_amount) - LAG(SUM(o.total_amount), 1) OVER (ORDER BY DATE_FORMAT(order_date, '%Y-%m'))) / LAG(SUM(o.total_amount), 1) OVER (ORDER BY DATE_FORMAT(order_date, '%Y-%m')) * 100 as growth_rate
            FROM orders o
            WHERE order_date > '2023-01-01'
            GROUP BY DATE_FORMAT(order_date, '%Y-%m')
            ORDER BY month""", False,
         "月度销售趋势分析"),

        # ===== 25. 机器学习特征工程模拟 =====
        ("特征工程", "客户特征提取",
         """SELECT c.id,
                   c.total_orders,
                   c.total_spent,
                   c.loyalty_points,
                   DATEDIFF(CURDATE(), c.customer_since) as customer_age_days,
                   (SELECT COUNT(DISTINCT o.product_id) FROM orders o WHERE o.customer_id = c.id) as unique_products_purchased,
                   (SELECT AVG(r.rating) FROM reviews r JOIN orders o ON r.customer_id = o.customer_id WHERE o.customer_id = c.id) as avg_review_rating,
                   (SELECT MAX(o.total_amount) FROM orders o WHERE o.customer_id = c.id) as max_order_value,
                   (SELECT STDDEV(o.total_amount) FROM orders o WHERE o.customer_id = c.id) as order_value_stddev,
                   CASE WHEN c.is_premium = 1 THEN 1 ELSE 0 END as is_premium_flag
            FROM customers c
            WHERE c.total_orders > 0
            LIMIT 100""", False,
         "机器学习特征提取"),

        # ===== 26. A/B测试模拟 =====
        ("A/B测试", "分组对比",
         """WITH customer_groups AS (
                SELECT id,
                       CASE WHEN MOD(id, 2) = 0 THEN 'Group_A' ELSE 'Group_B' END as test_group
                FROM customers
                WHERE total_orders > 0
            )
            SELECT cg.test_group,
                   COUNT(DISTINCT cg.id) as customer_count,
                   COUNT(DISTINCT o.id) as total_orders,
                   SUM(o.total_amount) as total_revenue,
                   AVG(o.total_amount) as avg_order_value,
                   AVG(DATEDIFF(o.order_date, c.customer_since)) as avg_days_to_first_order
            FROM customer_groups cg
            JOIN customers c ON cg.id = c.id
            LEFT JOIN orders o ON c.id = o.customer_id
            GROUP BY cg.test_group""", False,
         "A/B测试分组统计"),

        # ===== 27. 异常检测查询 =====
        # ("异常检测", "异常订单检测",
        #  """SELECT o.id, o.order_number, o.customer_id, o.total_amount,
        #            o.order_date, o.payment_method,
        #            (SELECT AVG(total_amount) FROM orders o2 WHERE o2.customer_id = o.customer_id) as customer_avg,
        #            o.total_amount - (SELECT AVG(total_amount) FROM orders o2 WHERE o2.customer_id = o.customer_id) as deviation_from_avg,
        #            CASE
        #                WHEN o.total_amount > 3 * (SELECT STDDEV(total_amount) FROM orders o2 WHERE o2.customer_id = o.customer_id) + (SELECT AVG(total_amount) FROM orders o2 WHERE o2.customer_id = o.customer_id) THEN 'High Anomaly'
        #                WHEN o.total_amount < (SELECT AVG(total_amount) FROM orders o2 WHERE o2.customer_id = o.customer_id) - 3 * (SELECT STDDEV(total_amount) FROM orders o2 WHERE o2.customer_id = o.customer_id) THEN 'Low Anomaly'
        #                ELSE 'Normal'
        #            END as anomaly_type
        #     FROM orders o
        #     WHERE (SELECT COUNT(*) FROM orders o2 WHERE o2.customer_id = o.customer_id) > 5
        #     HAVING anomaly_type != 'Normal'
        #     ORDER BY deviation_from_avg DESC
        #     LIMIT 20""", False,
        #  "异常订单检测"),

        # ===== 28. 递归查询实际应用 =====
        ("递归查询", "组织层级模拟",
         """WITH RECURSIVE org_hierarchy AS (
                SELECT id, first_name, last_name, department, position, 1 as level
                FROM employee_salaries 
                WHERE position LIKE '%Manager%'
                UNION ALL
                SELECT e.id, e.first_name, e.last_name, e.department, e.position, oh.level + 1
                FROM employee_salaries e
                JOIN org_hierarchy oh ON e.department = oh.department AND e.position NOT LIKE '%Manager%' AND e.id > oh.id
                WHERE oh.level < 3
            )
            SELECT * FROM org_hierarchy ORDER BY department, level""", False,
         "模拟组织层级结构"),

        # ===== 29. 全文本搜索模拟 =====
        ("全文搜索", "文本匹配",
         """SELECT r.id, r.title, r.content, r.rating,
                   p.name as product_name,
                   MATCH(r.title, r.content) AGAINST ('excellent quality' IN NATURAL LANGUAGE MODE) as relevance_score
            FROM reviews r
            JOIN products p ON r.product_id = p.id
            WHERE MATCH(r.title, r.content) AGAINST ('excellent quality' IN NATURAL LANGUAGE MODE)
            ORDER BY relevance_score DESC
            LIMIT 10""", False,
         "全文搜索匹配"),

        # ===== 30. 性能优化模式 =====
        ("性能优化", "覆盖索引查询",
         """SELECT category, brand, COUNT(*) 
            FROM products 
            WHERE category = 'Electronics' 
            GROUP BY category, brand""", False,
         "覆盖索引优化查询"),

        ("性能优化", "延迟关联",
         """SELECT p.* 
            FROM products p
            JOIN (SELECT id FROM products WHERE category = 'Electronics' ORDER BY rating DESC LIMIT 100 OFFSET 0) as tmp
            ON p.id = tmp.id""", False,
         "延迟关联优化"),

        # ===== 31. 边缘情况测试 =====
        ("边缘情况", "极端数值",
         "SELECT * FROM products WHERE price = 0 OR price > 999999.99", False,
         "极端数值查询"),

        ("边缘情况", "超长字符串",
         "SELECT * FROM products WHERE name LIKE '%" + "a" * 100 + "%'", False,
         "超长模式匹配"),

        ("边缘情况", "日期边界",
         "SELECT * FROM orders WHERE order_date = '0000-00-00' OR order_date = '9999-12-31'", False,
         "日期边界值"),

        ("边缘情况", "空集合操作",
         "SELECT * FROM products WHERE id IN (SELECT id FROM products WHERE price > 1000000)", False,
         "空子查询结果"),

        # ===== 32. 并发测试模拟 =====
        ("并发模拟", "行级锁模拟",
         "SELECT * FROM products WHERE id = 1 FOR UPDATE", True,
         "FOR UPDATE锁机制"),

        ("并发模拟", "乐观锁检查",
         "UPDATE products SET stock = stock - 1, version = version + 1 WHERE id = 1 AND version = 1", True,
         "乐观锁更新模式"),

        # ===== 33. 审计和日志查询 =====
        ("审计查询", "用户行为分析",
         """SELECT user_id, username, action, table_name,
                   COUNT(*) as action_count,
                   SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) as success_count,
                   SUM(CASE WHEN success = 0 THEN 1 ELSE 0 END) as failure_count
            FROM audit_logs 
            WHERE log_date > DATE_SUB(NOW(), INTERVAL 7 DAY)
            GROUP BY user_id, username, action, table_name
            HAVING COUNT(*) > 10
            ORDER BY action_count DESC""", True,
         "用户行为审计分析（应拦截）"),

        # ===== 34. 数据质量检查 =====
        ("数据质量", "完整性检查",
         """SELECT 'products' as table_name,
                   COUNT(*) as total_rows,
                   SUM(CASE WHEN name IS NULL OR name = '' THEN 1 ELSE 0 END) as missing_names,
                   SUM(CASE WHEN price IS NULL OR price <= 0 THEN 1 ELSE 0 END) as invalid_prices,
                   SUM(CASE WHEN stock IS NULL OR stock < 0 THEN 1 ELSE 0 END) as invalid_stock
            FROM products
            UNION ALL
            SELECT 'customers' as table_name,
                   COUNT(*) as total_rows,
                   SUM(CASE WHEN email IS NULL OR email = '' THEN 1 ELSE 0 END) as missing_emails,
                   SUM(CASE WHEN email NOT LIKE '%@%.%' THEN 1 ELSE 0 END) as invalid_emails,
                   SUM(CASE WHEN total_orders IS NULL OR total_orders < 0 THEN 1 ELSE 0 END) as invalid_order_counts
            FROM customers""", False,
         "数据质量完整性检查"),

        # ===== 35. 跨数据库模式模拟 =====
        ("跨库模拟", "联邦查询模拟",
         """SELECT p.name, o.total_amount, c.country
            FROM products p
            LEFT JOIN (
                SELECT product_id, SUM(total_amount) as total_amount 
                FROM orders 
                GROUP BY product_id
            ) o ON p.id = o.product_id
            LEFT JOIN (
                SELECT id, country 
                FROM suppliers
            ) s ON p.supplier_id = s.id""", False,
         "模拟联邦查询模式"),

        # ===== 36. 动态SQL构建测试 =====
        ("动态SQL", "条件构建",
         """SELECT * FROM products 
            WHERE 1=1
              AND category = 'Electronics'
              AND price > 100
              AND (stock > 0 OR warehouse = 'WH-North')
              AND (brand = 'BrandA' OR brand = 'BrandB' OR rating > 4.0)""", False,
         "动态条件构建模式"),

        # ===== 37. 数据加密字段测试 =====
        ("加密字段", "哈希查询",
         "SELECT username, MD5(password) as password_hash FROM users_secrets", True,
         "加密字段访问"),

        # ===== 38. 存储过程和函数测试 =====
        ("存储过程", "函数调用",
         "SELECT GET_LOCK('my_lock', 10)", True,
         "获取锁函数"),

        ("存储过程", "系统变量",
         "SELECT @old_price := price FROM products WHERE id = 1", True,
         "用户变量赋值"),

        # ===== 39. 分区表查询模拟 =====
        ("分区模拟", "分区查询",
         """SELECT * FROM orders 
            WHERE order_date BETWEEN '2024-01-01' AND '2024-01-31'
              AND total_amount > 1000
            ORDER BY order_date DESC""", False,
         "模拟分区表查询"),

        # ===== 40. 物化视图模式 =====
        ("物化视图", "汇总表查询",
         """SELECT category, 
                   SUM(daily_sales) as monthly_sales,
                   AVG(daily_sales) as avg_daily_sales,
                   COUNT(DISTINCT sale_date) as days_with_sales
            FROM (
                SELECT p.category, DATE(o.order_date) as sale_date, SUM(o.total_amount) as daily_sales
                FROM products p
                JOIN orders o ON p.id = o.product_id
                WHERE o.order_date > '2024-01-01'
                GROUP BY p.category, DATE(o.order_date)
            ) daily_sales
            GROUP BY category
            ORDER BY monthly_sales DESC""", False,
         "模拟物化视图查询"),

        # ===== 41. 图数据库模式模拟 =====
        ("图模式", "关系查询",
         """SELECT c1.first_name as customer1, 
                   c2.first_name as customer2,
                   COUNT(DISTINCT o1.product_id) as common_products
            FROM customers c1
            JOIN orders o1 ON c1.id = o1.customer_id
            JOIN orders o2 ON o1.product_id = o2.product_id
            JOIN customers c2 ON o2.customer_id = c2.id
            WHERE c1.id < c2.id
            GROUP BY c1.id, c2.id
            HAVING COUNT(DISTINCT o1.product_id) > 5
            ORDER BY common_products DESC
            LIMIT 10""", False,
         "模拟图关系查询"),

        # ===== 42. 时间序列预测模拟 =====
        ("时间序列", "移动平均预测",
         """WITH daily_sales AS (
                SELECT DATE(order_date) as sale_date,
                       SUM(total_amount) as daily_revenue
                FROM orders
                WHERE order_date > DATE_SUB(CURDATE(), INTERVAL 90 DAY)
                GROUP BY DATE(order_date)
            )
            SELECT sale_date, daily_revenue,
                   AVG(daily_revenue) OVER (ORDER BY sale_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as weekly_ma,
                   AVG(daily_revenue) OVER (ORDER BY sale_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) as monthly_ma,
                   daily_revenue - AVG(daily_revenue) OVER (ORDER BY sale_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as deviation_from_weekly_avg
            FROM daily_sales
            ORDER BY sale_date DESC""", False,
         "时间序列移动平均"),

        # ===== 43. 多维分析查询 =====
        ("多维分析", "数据立方体模拟",
         """SELECT COALESCE(category, 'ALL') as category,
                   COALESCE(brand, 'ALL') as brand,
                   COALESCE(WAREHOUSE, 'ALL') as warehouse,
                   COUNT(*) as product_count,
                   SUM(stock) as total_stock,
                   AVG(price) as avg_price,
                   MIN(price) as min_price,
                   MAX(price) as max_price
            FROM products
            GROUP BY category, brand, warehouse WITH ROLLUP
            HAVING category IS NOT NULL
            ORDER BY category, brand, warehouse""", False,
         "模拟多维数据立方体"),

        # ===== 44. 实时推荐系统模拟 =====
        ("推荐系统", "协同过滤模拟",
         """SELECT p1.id as product1, p2.id as product2,
                   COUNT(DISTINCT o.customer_id) as co_purchase_count,
                   COUNT(DISTINCT o.customer_id) * 1.0 / SQRT(
                       (SELECT COUNT(DISTINCT customer_id) FROM orders WHERE product_id = p1.id) *
                       (SELECT COUNT(DISTINCT customer_id) FROM orders WHERE product_id = p2.id)
                   ) as similarity_score
            FROM products p1
            JOIN orders o1 ON p1.id = o1.product_id
            JOIN orders o2 ON o1.customer_id = o2.customer_id
            JOIN products p2 ON o2.product_id = p2.id
            WHERE p1.id < p2.id AND p1.category = p2.category
            GROUP BY p1.id, p2.id
            HAVING co_purchase_count > 5
            ORDER BY similarity_score DESC
            LIMIT 20""", False,
         "模拟协同过滤推荐"),

        # ===== 45. 异常交易检测 =====
        ("欺诈检测", "异常交易模式",
         """SELECT o1.order_number, o1.customer_id, o1.total_amount, o1.order_date,
                   o2.order_number as previous_order, o2.order_date as previous_date,
                   TIMESTAMPDIFF(MINUTE, o2.order_date, o1.order_date) as minutes_between_orders,
                   o1.total_amount / NULLIF(o2.total_amount, 0) as amount_ratio
            FROM orders o1
            JOIN orders o2 ON o1.customer_id = o2.customer_id 
                          AND o2.order_date < o1.order_date
            WHERE TIMESTAMPDIFF(MINUTE, o2.order_date, o1.order_date) < 5
              AND o1.total_amount > 3 * o2.total_amount
            ORDER BY minutes_between_orders
            LIMIT 20""", False,
         "异常交易时间模式检测"),
    ]

    # 添加压力测试用例
    pressure_tests = [
        ("压力测试", "超多表JOIN",
         """SELECT p.name, o.order_number, c.first_name, s.company_name, r.rating, il.operation_type
            FROM products p
            JOIN orders o ON p.id = o.product_id
            JOIN customers c ON o.customer_id = c.id
            JOIN suppliers s ON p.supplier_id = s.id
            LEFT JOIN reviews r ON p.id = r.product_id AND c.id = r.customer_id
            LEFT JOIN inventory_logs il ON p.id = il.product_id
            WHERE p.category = 'Electronics' 
              AND o.order_date > '2024-01-01'
            ORDER BY o.total_amount DESC
            LIMIT 100""", False,
         "六表关联复杂查询"),

        ("压力测试", "超大GROUP BY",
         """SELECT category, brand, warehouse, supplier_id,
                   COUNT(*) as count,
                   SUM(price) as total_price,
                   AVG(price) as avg_price,
                   SUM(stock) as total_stock,
                   MIN(rating) as min_rating,
                   MAX(rating) as max_rating
            FROM products
            GROUP BY category, brand, warehouse, supplier_id
            HAVING COUNT(*) > 1
            ORDER BY category, brand, total_stock DESC""", False,
         "多维GROUP BY聚合"),
    ]

    test_cases.extend(pressure_tests)

    # 添加性能边界测试
    performance_boundary_tests = [
        ("性能边界", "无限递归尝试",
         """WITH RECURSIVE infinite AS (
                SELECT 1 as n
                UNION ALL
                SELECT n + 1 FROM infinite
            )
            SELECT * FROM infinite""", True,
         "无限递归CTE"),

        ("性能边界", "笛卡尔积攻击",
         "SELECT * FROM products p1, products p2, products p3 LIMIT 1000000", True,
         "三表笛卡尔积攻击"),

        ("性能边界", "超大IN列表",
         f"SELECT * FROM products WHERE id IN ({','.join(str(i) for i in range(1, 10001))})", True,
         "超长IN列表查询"),
    ]

    test_cases.extend(performance_boundary_tests)

    print(f"✅ 已生成 {len(test_cases)} 个测试用例，覆盖了：")
    print("  1. 基础查询（WHERE、JOIN、子查询、聚合）")
    print("  2. 高级功能（窗口函数、CTE、递归查询）")
    print("  3. 安全测试（SQL注入、危险函数、黑名单拦截）")
    print("  4. 性能测试（大查询、复杂JOIN、边界条件）")
    print("  5. 业务逻辑（库存预警、客户分析、供应商绩效）")
    print("  6. 特殊场景（时间序列、推荐系统、异常检测）")
    print("  7. 压力测试（多表关联、复杂聚合）")
    print("  8. 边界测试（极端数值、空查询、超长查询）")

    # 统计变量
    total = len(test_cases)
    passed = 0
    failed = 0
    category_stats = {}

    print("\n" + "="*80)
    print("🔒 SQL 安全审计测试套件")
    print("="*80)
    print(f"测试时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"总测试用例: {total}")
    print("="*80 + "\n")

    for category, name, sql, expect_fail, description in test_cases:
        # 统计分类
        if category not in category_stats:
            category_stats[category] = {"total": 0, "passed": 0, "failed": 0}
        category_stats[category]["total"] += 1
        
        print(f"📋 测试类别: [{category}]")
        print(f"📝 测试名称: {name}")
        print(f"💡 测试说明: {description}")
        print(f"📄 原始 SQL: {sql}")
        
        try:
            # 1. 验证与改写
            safe_sql = checker.validata(sql, 'mysql')
            print(f"✏️  改写后 SQL: {safe_sql}")
            
            # 2. 执行
            results = executor.execute(safe_sql)
            result_count = len(results)
            print(f"✅ 执行结果: 成功返回 {result_count} 行数据")
            
            # 显示部分结果
            if results and result_count > 0:
                print(f"📊 示例数据 (最多显示3行):")
                for i, row in enumerate(results[:3]):
                    print(f"   Row {i+1}: {row}")
            
            if expect_fail:
                print("❌ 测试失败：该 SQL 应该被拦截但通过了！")
                failed += 1
                category_stats[category]["failed"] += 1
            else:
                print("✅ 测试通过")
                passed += 1
                category_stats[category]["passed"] += 1
                
        except Exception as e:
            error_msg = str(e)
            if expect_fail:
                print(f"✅ 测试通过：成功拦截")
                print(f"🛡️  拦截原因: {error_msg}")
                passed += 1
                category_stats[category]["passed"] += 1
            else:
                print(f"❌ 测试失败：正常的 SQL 被误拦")
                print(f"⚠️  错误信息: {error_msg}")
                failed += 1
                category_stats[category]["failed"] += 1
        
        print("-" * 80 + "\n")

    # 输出测试总结
    print("="*80)
    print("📊 测试总结报告")
    print("="*80)
    print(f"总用例数: {total}")
    print(f"通过: {passed} ✅")
    print(f"失败: {failed} ❌")
    print(f"通过率: {(passed/total*100):.2f}%")
    print("\n分类统计:")
    print("-" * 80)
    for cat, stats in category_stats.items():
        pass_rate = (stats['passed'] / stats['total'] * 100) if stats['total'] > 0 else 0
        print(f"  {cat:20s} | 总计: {stats['total']:2d} | 通过: {stats['passed']:2d} | 失败: {stats['failed']:2d} | 通过率: {pass_rate:6.2f}%")
    print("="*80)

    return passed, failed, total

if __name__ == "__main__":
    print("🚀 开始初始化测试环境...\n")
    setup_test_data()
    print("\n🧪 开始执行测试用例...\n")
    passed, failed, total = run_tests()
    
    if failed == 0:
        print("\n🎉 所有测试通过！系统安全防护运行正常。")
    else:
        print(f"\n⚠️  有 {failed} 个测试失败，请检查安全策略配置。")