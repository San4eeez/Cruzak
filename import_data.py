import pandas as pd
import psycopg2
from tqdm import tqdm


# Функция для подключения к PostgreSQL
def connect_to_db():
    # Установите свои параметры подключения
    connection = psycopg2.connect(
        host="localhost",
        database="Cruzak",
        user="postgres",
        password="admin"
    )
    return connection


# Функция для создания таблиц
def create_tables(connection):
    cursor = connection.cursor()
    # Удалить существующие таблицы, если они есть
    cursor.execute("DROP TABLE IF EXISTS products CASCADE;")
    cursor.execute("DROP TABLE IF EXISTS product_attributes CASCADE;")

    # Создать таблицу products
    cursor.execute("""
        CREATE TABLE products (
            id SERIAL PRIMARY KEY,
            name TEXT,
            okpd2 TEXT,
            detail TEXT,
            unit TEXT,
            category TEXT,
            ktru_code TEXT,
            kkn_code TEXT,
            product_part TEXT,
            update_date DATE,
            is_russian BOOLEAN
        );
    """)

    # Создать таблицу product_attributes
    cursor.execute("""
        CREATE TABLE product_attributes (
            id SERIAL PRIMARY KEY,
            product_id INTEGER REFERENCES products(id),
            attribute_name TEXT,
            numeric_value NUMERIC,
            text_value TEXT,
            additional_text_value TEXT,
            unit TEXT
        );
    """)
    connection.commit()


# Функция для импорта данных
def import_data(file_path, connection):
    # Прочитать данные из Excel
    df = pd.read_excel(file_path, header=None, skiprows=4)

    current_product = None
    product_data = {}
    attributes = []

    for index, row in tqdm(df.iterrows(), total=len(df), desc="Processing rows"):
        # Проверить, является ли текущая строка названием товара
        if pd.notna(row[1]):  # 2-й столбец (название товара)
            # Если есть текущий товар, сохранить его данные
            if current_product:
                product_id = save_product(product_data, connection)
                save_attributes(attributes, product_id, connection)

            # Начать новый товар
            current_product = row[1]
            product_data = {
                "name": row[1],
                "okpd2": row[2],
                "detail": row[3],
                "unit": row[4],
                "category": row[11],
                "ktru_code": row[12],
                "kkn_code": row[13],
                "product_part": row[14],
                "update_date": pd.to_datetime(row[15]).date(),
                "is_russian": row[16] == "Да"
            }
            attributes = []
        else:
            # Обработка характеристик товара
            attribute_name = row[5]
            numeric_value = row[6] if pd.notna(row[6]) else None
            text_value = row[8] if pd.notna(row[8]) else None
            additional_text_value = row[9] if pd.notna(row[9]) else None
            unit = row[10] if pd.notna(row[10]) else None

            # Разделить текстовые значения, если они разделены точкой с запятой
            if text_value and isinstance(text_value, str) and ";" in text_value:
                text_values = [value.strip() for value in text_value.split(";")]
            else:
                text_values = [text_value] if text_value else []

            # Преобразовать числовые значения
            if numeric_value and isinstance(numeric_value, str):
                numeric_value = numeric_value.replace(",", ".")
                try:
                    numeric_value = float(numeric_value)
                except ValueError:
                    numeric_value = None

            attributes.append({
                "attribute_name": attribute_name,
                "numeric_value": numeric_value,
                "text_values": text_values,
                "additional_text_value": additional_text_value,
                "unit": unit
            })

    # Сохранить последний товар
    if current_product:
        product_id = save_product(product_data, connection)
        save_attributes(attributes, product_id, connection)


# Функция для сохранения товара
def save_product(product_data, connection):
    cursor = connection.cursor()
    print(f"Saving product: {product_data['name']}")
    cursor.execute("""
        INSERT INTO products (name, okpd2, detail, unit, category, ktru_code, kkn_code, product_part, update_date, is_russian)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id;
    """, (
        product_data["name"],
        product_data["okpd2"],
        product_data["detail"],
        product_data["unit"],
        product_data["category"],
        product_data["ktru_code"],
        product_data["kkn_code"],
        product_data["product_part"],
        product_data["update_date"],
        product_data["is_russian"]
    ))
    product_id = cursor.fetchone()[0]
    connection.commit()
    print(f"Product ID: {product_id} saved successfully.")
    return product_id


# Функция для сохранения характеристик товара
def save_attributes(attributes, product_id, connection):
    cursor = connection.cursor()
    for attr in attributes:
        for text_value in attr["text_values"]:
            print(f"Saving attribute: {attr['attribute_name']} for product ID: {product_id}, text_value: {text_value}")
            cursor.execute("""
                INSERT INTO product_attributes (product_id, attribute_name, numeric_value, text_value, additional_text_value, unit)
                VALUES (%s, %s, %s, %s, %s, %s);
            """, (
                product_id,
                attr["attribute_name"],
                attr["numeric_value"],
                text_value,
                attr["additional_text_value"],
                attr["unit"]
            ))
    connection.commit()
    print(f"Attributes for product ID: {product_id} saved successfully.")


# Главная функция
def main():
    file_path = "part.xlsx"
    connection = connect_to_db()
    create_tables(connection)
    import_data(file_path, connection)
    connection.close()


if __name__ == "__main__":
    main()