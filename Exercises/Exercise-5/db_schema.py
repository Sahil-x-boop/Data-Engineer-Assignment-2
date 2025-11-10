def create_tables(conn):
    ddl = """
        CREATE TABLE IF NOT EXISTS accounts (
            customer_id INT PRIMARY KEY,
            first_name VARCHAR(50),
            last_name VARCHAR(50),
            address_1 VARCHAR(100),
            address_2 VARCHAR(100),
            city VARCHAR(50),
            state VARCHAR(50),
            zip_code VARCHAR(20),
            join_date DATE
        );

        CREATE TABLE IF NOT EXISTS products (
            product_id INT PRIMARY KEY,
            product_code VARCHAR(10),
            product_description VARCHAR(100)
        );

        CREATE TABLE IF NOT EXISTS transactions (
            transaction_id VARCHAR(50) PRIMARY KEY,
            transaction_date DATE,
            product_id INT,
            product_code VARCHAR(10),
            product_description VARCHAR(100),
            quantity INT,
            account_id INT,
            FOREIGN KEY (product_id) REFERENCES products(product_id) ON DELETE CASCADE,
            FOREIGN KEY (account_id) REFERENCES accounts(customer_id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_transactions_product_id ON transactions(product_id);
        CREATE INDEX IF NOT EXISTS idx_transactions_account_id ON transactions(account_id);
        """
    
    with conn.cursor() as cur:
        cur.execute(ddl)
        conn.commit()
        print("Tables created")

