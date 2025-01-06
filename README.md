# История выполнения ETL-проекта: Used Cars Data from eBay Kleinanzeigen

## Описание проекта

В данном проекте я выполнил полный процесс ETL (Extract, Transform, Load) для данных о подержанных автомобилях, предоставленных с платформы eBay Kleinanzeigen. Основной целью было преобразование сырых данных в структуру реляционной базы данных, пригодную для анализа и обработки.

Процесс был реализован с использованием следующих технологий:

- **PySpark** для обработки данных.
- **PostgreSQL** для хранения данных.
- **Google Colab** для разработки и выполнения кода.
- **Apache Airflow** для автоматизации ETL-процесса.

---

## Этапы выполнения проекта

### 1. Подготовка окружения и настройка зависимостей

Первоначально была выполнена настройка окружения и установка всех необходимых библиотек.

```python
# Подключение Google Drive
from google.colab import drive
drive.mount('/content/drive')

# Установка зависимостей
!pip install pyspark psycopg2-binary apache-airflow

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

1. **Удаление дубликатов**: дубликаты могут искажать статистику и мешать анализу.
2. **Удаление строк с пропущенными значениями в критических столбцах**: такие значения делают запись неполной и бесполезной.
3. **Замену пропущенных значений в категориальных столбцах на "Other"**: это позволяет сохранить данные, не искажая их логику.
4. **Удаление строк с ценой равной 0**: такие значения некорректны, так как автомобили не могут стоить ноль.
5. **Ограничение диапазона для года регистрации автомобилей (1900–2025)**: значения за пределами этого диапазона считаются ошибочными.
6. **Удаление столбца `nrOfPictures`**: он содержит только нули и не несет полезной информации.

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

Данные были разделены на несколько связанных таблиц для загрузки в реляционную базу данных. Нормализация помогает устранить избыточность данных и улучшить производительность запросов.

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

### 5. Создание DAG для автоматизации ETL-процесса

Был реализован DAG с использованием Apache Airflow, чтобы автоматизировать процесс ETL. DAG включает три задачи:

1. **Extract**: загрузка данных из исходного CSV.
2. **Transform**: очистка и нормализация данных.
3. **Load**: загрузка обработанных данных в PostgreSQL.

```python
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
import pandas as pd
from pyspark.sql import SparkSession

# Инициализация Spark
def init_spark():
    return SparkSession.builder \
        .appName("UsedCarsETL") \
        .config("spark.master", "local[*]") \
        .getOrCreate()

# Шаг 1: Загрузка данных
def extract_data(**kwargs):
    spark = init_spark()
    file_path = "/content/drive/MyDrive/Datasets/used_cars.csv"
    kwargs['ti'].xcom_push(key='raw_data', value=file_path)

# Шаг 2: Трансформация данных
def transform_data(**kwargs):
    spark = init_spark()
    raw_data_path = kwargs['ti'].xcom_pull(task_ids='extract_data', key='raw_data')
    df = spark.read.csv(raw_data_path, header=True, inferSchema=True)
    # Очистка и нормализация данных

# Шаг 3: Загрузка данных в PostgreSQL
def load_data(**kwargs):
    # Логика загрузки данных в базу

# DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'used_cars_etl',
    default_args=default_args,
    description='ETL pipeline for Used Cars dataset',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    provide_context=True,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    provide_context=True,
    dag=dag,
)

extract_task >> transform_task >> load_task
```

### 6. Примеры запросов

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

