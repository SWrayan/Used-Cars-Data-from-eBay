# История выполнения ETL-проекта: Used Cars Data from eBay Kleinanzeigen

## Описание проекта

В данном проекте я выполнил полный процесс ETL (Extract, Transform, Load) для данных о подержанных автомобилях, предоставленных с платформы eBay Kleinanzeigen. Основной целью было преобразование сырых данных в структуру реляционной базы данных, пригодную для анализа и обработки.

Процесс был реализован с использованием следующих технологий:

- **PySpark** для обработки данных.
- **PostgreSQL** для хранения данных.
- **Google Colab** для разработки и выполнения кода.

---

## Этапы выполнения проекта

### 1. Подготовка окружения и настройка зависимостей

Первоначально была выполнена настройка окружения и установка всех необходимых библиотек.

```python
# Подключение Google Drive
from google.colab import drive
drive.mount('/content/drive')

# Установка зависимостей
!pip install pyspark psycopg2-binary

# Импорт библиотек
from pyspark.sql import SparkSession
import psycopg2
```

SparkSession был инициализирован:

```python
spark = SparkSession.builder \
    .appName("Used Cars ETL") \
    .config("spark.master", "local[*]") \
    .getOrCreate()
```

### 2. Загрузка и первичная обработка данных

Датасет был загружен с Google Drive:

```python
file_path = '/content/drive/MyDrive/Datasets/used_cars.csv'
df = spark.read.csv(file_path, header=True, inferSchema=True)
df.printSchema()
df.show(5)
```

Проверка структуры и качества данных включала поиск дубликатов, пропущенных значений и неадекватных значений в числовых столбцах.

```python
# Проверка дубликатов
duplicate_count = df.count() - df.dropDuplicates().count()
print(f"Число дубликатов: {duplicate_count}")

# Проверка пропущенных значений
from pyspark.sql import functions as F
missing_values = df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns])
missing_values.show()
```

### 3. Очистка данных

Были предприняты следующие шаги для очистки данных:

1. Удаление дубликатов.
2. Удаление строк с пропущенными значениями в критических столбцах.
3. Замену пропущенных значений в категориальных столбцах на "Other".
4. Удаление строк с ценой равной 0.
5. Ограничение диапазона для года регистрации автомобилей (1900–2025).
6. Удаление столбца `nrOfPictures`.

```python
# Удаление дубликатов
df = df.dropDuplicates()

# Очистка критических столбцов
critical_columns = ['seller', 'offerType', 'price', 'yearOfRegistration']
df = df.dropna(subset=critical_columns)

# Заменяем null на "Other" для категориальных столбцов
categorical_columns = ['vehicleType', 'gearbox', 'fuelType', 'notRepairedDamage']
for col in categorical_columns:
    df = df.fillna({col: 'Other'})

# Удаление строк с ценой равной 0
df = df.filter(F.col("price") > 0)

# Ограничение диапазона года регистрации
df = df.filter((F.col("yearOfRegistration") >= 1900) & (F.col("yearOfRegistration") <= 2025))

# Удаление ненужного столбца
df = df.drop("nrOfPictures")
```

### 4. Нормализация данных

Данные были разделены на несколько связанных таблиц для загрузки в реляционную базу данных.

```python
# Таблица sellers
df_sellers = df.select("seller", "offerType").distinct() \
    .withColumn("seller_id", F.monotonically_increasing_id())

# Таблица vehicles
df_vehicles = df.select("vehicleType", "brand", "model", "yearOfRegistration", "powerPS", "fuelType").distinct() \
    .withColumn("vehicle_id", F.monotonically_increasing_id())

# Таблица geolocation
df_geolocation = df.select("postalCode").distinct() \
    .withColumn("location_id", F.monotonically_increasing_id())

# Таблица listings
df_listings = df.join(df_sellers, ["seller", "offerType"], "left") \
    .join(df_vehicles, ["vehicleType", "brand", "model", "yearOfRegistration", "powerPS", "fuelType"], "left") \
    .join(df_geolocation, "postalCode", "left") \
    .select("dateCrawled", "price", "kilometer", "monthOfRegistration", "notRepairedDamage", "dateCreated", "lastSeen", "vehicle_id", "seller_id", "location_id") \
    .withColumn("listing_id", F.monotonically_increasing_id())
```

### 5. Создание таблиц в PostgreSQL

Были созданы таблицы в базе данных PostgreSQL:

```python
cursor.execute("""
CREATE TABLE IF NOT EXISTS Sellers (
    seller_id SERIAL PRIMARY KEY,
    seller TEXT,
    offerType TEXT
);
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS Vehicles (
    vehicle_id SERIAL PRIMARY KEY,
    vehicleType TEXT,
    brand TEXT,
    model TEXT,
    yearOfRegistration INT,
    powerPS INT,
    fuelType TEXT
);
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS Geolocation (
    location_id SERIAL PRIMARY KEY,
    postalCode INT
);
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS Listings (
    listing_id SERIAL PRIMARY KEY,
    dateCrawled TIMESTAMP,
    price INT,
    kilometer INT,
    monthOfRegistration INT,
    notRepairedDamage BOOLEAN,
    dateCreated TIMESTAMP,
    lastSeen TIMESTAMP,
    vehicle_id INT REFERENCES Vehicles(vehicle_id),
    seller_id INT REFERENCES Sellers(seller_id),
    location_id INT REFERENCES Geolocation(location_id)
);
""")
conn.commit()
```

### 6. Загрузка данных в базу данных

Данные были загружены в соответствующие таблицы.

```python
from psycopg2.extras import execute_values

def insert_data(df, table_name):
    data = df.collect()
    columns = df.columns
    execute_values(
        cursor,
        f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES %s",
        [[getattr(row, col) for col in columns] for row in data]
    )
    conn.commit()

insert_data(df_sellers, "Sellers")
insert_data(df_vehicles, "Vehicles")
insert_data(df_geolocation, "Geolocation")
insert_data(df_listings, "Listings")
```

### 7. Примеры запросов

Были выполнены запросы для анализа данных.

```python
# Топ-5 самых популярных брендов
cursor.execute("""
SELECT brand, COUNT(*) AS count
FROM Vehicles v
JOIN Listings l ON v.vehicle_id = l.vehicle_id
GROUP BY brand
ORDER BY count DESC
LIMIT 5;
""")
print(cursor.fetchall())

# Средняя цена автомобилей по типу топлива
cursor.execute("""
SELECT fuelType, AVG(price) AS avg_price
FROM Vehicles v
JOIN Listings l ON v.vehicle_id = l.vehicle_id
GROUP BY fuelType
ORDER BY avg_price DESC;
""")
print(cursor.fetchall())
```

---

## Выводы

Данный проект продемонстрировал навыки работы с большими объемами данных, включая их обработку, нормализацию и загрузку в реляционную базу данных. Полученная структура данных позволяет эффективно анализировать информацию и извлекать полезные инсайты.

