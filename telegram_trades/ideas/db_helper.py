import duckdb
from typing import Tuple


class Duck:

    def __init__(self, db_name="telegram_trades.db"):
        self.con = duckdb.connect(
            database=db_name, read_only=False)

    def insert_values(self, table, values: Tuple) -> None:
        self.con.execute(
            f"INSERT INTO {table} VALUES {values}")

    def select_values(self, sql) -> Tuple:
        return self.con.execute(sql).fetchall()

    def update(self, sql) -> None:
        self.con.execute(sql)


if __name__ == "__main__":
    db = Duck()
    sql = "CREATE TABLE IF NOT EXISTS tbl_message( " \
        "id INTEGER, symbol VARCHAR(155), entry DECIMAL(5,2), " \
        "stop1 DECIMAL(5,2), target1 DECIMAL(5,2), " \
        "target2 DECIMAL(5,2), product VARCHAR(4), fn VARCHAR(25))"

    sql = "CREATE TABLE IF NOT EXISTS tbl_order( " \
        "order_id BIGINT, parent_id INTEGER, leg INTEGER, " \
        "is_entry BOOLEAN, side VARCHAR(4), order_type VARCHAR(8), " \
        "price DECIMAL(5,2), quantity INTEGER, trigger_price DECIMAL(5,2), " \

    db.con.execute(sql)
    db.con.execute("CREATE SEQUENCE IF NOT EXISTS seq_order_id START 1")
    tpl = (12334, 136, 1, True, 'BTC', 101.00,
           1, 102.00, 'Buy', 'NRML', 'MARKET', 'next_fn')
    db.insert_values("tbl_order", tpl)
    res = db.con.sql("SELECT * FROM tbl_order")
    print(res)
