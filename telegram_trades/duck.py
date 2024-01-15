import duckdb


class Duck:

    def __init__(self, db_name="telegram_trades.db"):
        self.con = duckdb.connect(
            database=db_name, read_only=False)

    def insert_values(self, table, values):
        self.con.execute(
            f"INSERT INTO {table} VALUES {values}")


if __name__ == "__main__":
    db = Duck()
    sql = "CREATE TABLE IF NOT EXISTS tbl_order( " \
        "order_id INTEGER, " \
        "parent_id BIGINT, leg BIGINT, " \
        "is_entry BOOLEAN, symbol VARCHAR(155), price DECIMAL(5, 2), "  \
        "quantity INTEGER, trigger_price DECIMAL(5, 2), " \
        "side VARCHAR(4), product VARCHAR(4), order_type VARCHAR(8), " \
        "next_fn VARCHAR(50))"
    db.con.execute(sql)
    db.con.execute("CREATE SEQUENCE IF NOT EXISTS seq_order_id START 1")
    tpl = (12334, 136, 1, True, 'BTC', 101.00,
           1, 102.00, 'Buy', 'NRML', 'MARKET', 'next_fn')
    db.insert_values("tbl_order", tpl)
    res = db.con.sql("SELECT * FROM tbl_order")
    print(res)
