import time

from pydruid.db import connect

def read_ab_config():
    with open('./apps/ab_config', 'r') as f:
        lines = f.readlines()
        return int(lines[0])

def update_ab_config(n):
    with open('./apps/ab_config', 'w') as f:
        f.write(str(n))

def main():
    weights = {"discount-at-home-page": 0, "discount-at-category-page": 0}
    percentage_change_amount = 5

    while True:
        conn = connect(host='localhost', port=8082, path='/druid/v2/sql/', scheme='http')
        curs = conn.cursor()
        curs.execute("""
            SELECT COUNT(*) as "sales-conversion-count", "id"
            FROM "sales-by-discount"
            GROUP BY "id"
            ORDER BY "sales-conversion-count"
        """)

        discount_at_home_page_percentage= read_ab_config()

        if discount_at_home_page_percentage == 100:
            break

        for row in curs:
            weights[row[1]] = row[0]

        print(weights)
        
        if weights["discount-at-home-page"] > weights["discount-at-category-page"]:
            update_ab_config(discount_at_home_page_percentage + percentage_change_amount)
        else:
            update_ab_config(discount_at_home_page_percentage - percentage_change_amount)

        print(f"discount_at_home_page_percentage updated to {read_ab_config()}")

        time.sleep(60)

if __name__ == "__main__":
    main()