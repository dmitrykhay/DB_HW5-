import psycopg2


def create_tables(conn):
    with conn.cursor() as cur:
        cur.execute('''
            CREATE TABLE IF NOT EXISTS clients 
            (id SERIAL PRIMARY KEY, 
            first_name VARCHAR(50) NOT NULL, 
            last_name VARCHAR(50) NOT NULL, 
            email VARCHAR(50) NOT NULL);
        ''')
        cur.execute('''
            CREATE TABLE IF NOT EXISTS phone_numbers
            (id SERIAL PRIMARY KEY,
            client_id INTEGER NOT NULL REFERENCES clients(id),
            number VARCHAR(20) NOT NULL);
        ''')
        print("Tables created successfully\n")
        conn.commit()


def add_client(conn, first_name, last_name, email, number=None):
    with conn.cursor() as cur:
        cur.execute('''
        INSERT INTO clients(first_name, last_name, email) 
        VALUES(%s, %s, %s);
        ''', (first_name, last_name, email))
        conn.commit()
        cur.execute('''
        SELECT id FROM clients WHERE email=%s
        ''', (email,))
        client_id = cur.fetchone()[0]
        if number:
            cur.execute('''
            INSERT INTO phone_numbers(client_id, number) VALUES (%s, %s);
            ''', (client_id, number))
            conn.commit()


def add_number(conn, client_id, number):
    with conn.cursor() as cur:
        cur.execute('''
        INSERT INTO phone_numbers(client_id, number)
        VALUES(%s, %s)
        RETURNING id, client_id, number;
        ''', (client_id, number))
        conn.commit()


def change_client(conn, id, first_name=None, last_name=None, email=None, number=None):
    with conn.cursor() as cur:
        if first_name:
            cur.execute('''
            UPDATE clients SET first_name=%s WHERE id=%s;
            ''', (first_name, id))
        if last_name:
            cur.execute('''
            UPDATE clients SET last_name=%s WHERE id=%s;
            ''', (last_name, id))
        if email:
            cur.execute('''
            UPDATE clients SET email=%s WHERE id=%s;
            ''', (email, id))
        if number:
            cur.execute('''
            UPDATE phone_numbers SET number=%s WHERE id=%s;
            ''', (number, id))
        conn.commit()


def del_number(conn, client_id, number):
    with conn.cursor() as cur:
        cur.execute('''
        DELETE FROM phone_numbers WHERE client_id=%s AND number=%s;
        ''', (client_id, number))
    conn.commit()


def del_client(conn, id):
    with conn.cursor() as cur:
        cur.execute('''
        DELETE FROM clients WHERE id=%s;
        ''', (id,))
    conn.commit()


def find_client(conn, id=None, first_name=None, last_name=None, email=None, number=None):
    with conn.cursor() as cur:
        if id:
            cur.execute('''
            SELECT c.id, c.first_name, c.last_name, c.email, pn.number FROM clients c
            LEFT JOIN phone_numbers pn ON c.id=pn.client_id
            WHERE first_name=%s;
            ''', (id,))
            print(cur.fetchone())
        if first_name:
            cur.execute('''
            SELECT c.id, c.first_name, c.last_name, c.email, pn.number FROM clients c
            LEFT JOIN phone_numbers pn ON c.id=pn.client_id
            WHERE first_name=%s;
            ''', (first_name,))
            print(cur.fetchone())
        if last_name:
            cur.execute('''
            SELECT c.id, c.first_name, c.last_name, c.email, pn.number FROM clients c
            LEFT JOIN phone_numbers pn ON c.id=pn.client_id
            WHERE last_name=%s;
            ''', (last_name,))
            print(cur.fetchone())
        if email:
            cur.execute('''
            SELECT c.id, c.first_name, c.last_name, c.email, pn.number FROM clients c
            LEFT JOIN phone_numbers pn ON c.id=pn.client_id
            WHERE email=%s;
            ''', (email,))
            print(cur.fetchone())
        if email:
            cur.execute('''
            SELECT c.id, c.first_name, c.last_name, c.email, pn.number FROM clients c
            LEFT JOIN phone_numbers pn ON c.id=pn.client_id
            WHERE email=%s;
            ''', (email,))
            print(cur.fetchone())
        if number:
            cur.execute('''
            SELECT c.id, c.first_name, c.last_name, c.email, pn.number FROM clients c
            LEFT JOIN phone_numbers pn ON c.id=pn.client_id
            WHERE number=%s;
            ''', (number,))
            print(cur.fetchone())


def drop_tables(conn):
    with conn.cursor() as cur:
        cur.execute('''
        DROP TABLE phone_numbers;
        DROP TABLE clients;
        ''')
    conn.commit()


def show_all(conn):
    with conn.cursor() as cur:
        cur.execute('''
        SELECT c.first_name, c.last_name, c.email, pn.number FROM clients c
        LEFT JOIN phone_numbers pn ON c.id=pn.client_id;
        ''')
        rows = cur.fetchall()
        for row in rows:
            print("First name: ", row[0])
            print("Last name: ", row[1])
            print("e-mail: ", row[2])
            print("Phone number: ", row[3], "\n")


if __name__ == "__main__":
    with psycopg2.connect(
            database="Homework-5_db",
            user="postgres",
            password=" "
    ) as conn:
        drop_tables(conn)
        create_tables(conn)
        add_client(conn, "Dmitry", "Khayretdinov", "dmitry-K@icloud.com")
        add_client(conn, "Svetlana", "Arbuzova", "Svetlana-K@mail.ru", number=112)
        add_client(conn, "Ivan", "Ivanov", "Ivan@icloud.com")
        add_number(conn, "1", "89999857777")
        add_number(conn, "1", "89999858888")
        add_number(conn, "2", "89999859999")
        show_all(conn)
        print('x' * 30)
        change_client(conn, 2, last_name='Smirnova')
        change_client(conn, 1, email='iiv@mail.ru')
        show_all(conn)
        print('x' * 30)
        del_number(conn, "1", "89999857777")
        del_client(conn, "3")
        show_all(conn)
        print('x' * 30)
        find_client(conn, first_name="Svetlana", last_name="Smirnova")
        find_client(conn, number="89999858888")
conn.close()
