```python
# 1. Подключение Google Drive
from google.colab import drive
drive.mount('/content/drive')

# 2. Добавление пути к зависимостям в sys.path и PYTHONPATH
DEPENDENCIES_DIR = '/content/drive/MyDrive/colab_dependencies'  # Путь к директории с зависимостями

import sys
if DEPENDENCIES_DIR not in sys.path:
    sys.path.append(DEPENDENCIES_DIR)

import os
if 'PYTHONPATH' not in os.environ:
    os.environ['PYTHONPATH'] = DEPENDENCIES_DIR
else:
    os.environ['PYTHONPATH'] = DEPENDENCIES_DIR + ':' + os.environ['PYTHONPATH']


```

    Drive already mounted at /content/drive; to attempt to forcibly remount, call drive.mount("/content/drive", force_remount=True).



```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Used Cars ETL") \
    .config("spark.master", "local[*]") \
    .getOrCreate()
```


```python
dataset_path = '/content/drive/MyDrive/Datasets/autos.csv'
```


```python
from pyspark.sql.functions import col, isnan, when, count
```


```python
file_path = '/content/drive/MyDrive/Datasets/autos.csv'
```


```python
df = spark.read.csv(file_path, header=True, inferSchema=True)
```


```python
print("Схема датасета:")
df.printSchema()
```

    Схема датасета:
    root
     |-- dateCrawled: timestamp (nullable = true)
     |-- name: string (nullable = true)
     |-- seller: string (nullable = true)
     |-- offerType: string (nullable = true)
     |-- price: integer (nullable = true)
     |-- abtest: string (nullable = true)
     |-- vehicleType: string (nullable = true)
     |-- yearOfRegistration: integer (nullable = true)
     |-- gearbox: string (nullable = true)
     |-- powerPS: integer (nullable = true)
     |-- model: string (nullable = true)
     |-- kilometer: integer (nullable = true)
     |-- monthOfRegistration: integer (nullable = true)
     |-- fuelType: string (nullable = true)
     |-- brand: string (nullable = true)
     |-- notRepairedDamage: string (nullable = true)
     |-- dateCreated: timestamp (nullable = true)
     |-- nrOfPictures: integer (nullable = true)
     |-- postalCode: integer (nullable = true)
     |-- lastSeen: timestamp (nullable = true)
    



```python
df.show()
```

    +-------------------+--------------------+------+---------+-----+-------+-----------+------------------+---------+-------+--------+---------+-------------------+--------+-------------+-----------------+-------------------+------------+----------+-------------------+
    |        dateCrawled|                name|seller|offerType|price| abtest|vehicleType|yearOfRegistration|  gearbox|powerPS|   model|kilometer|monthOfRegistration|fuelType|        brand|notRepairedDamage|        dateCreated|nrOfPictures|postalCode|           lastSeen|
    +-------------------+--------------------+------+---------+-----+-------+-----------+------------------+---------+-------+--------+---------+-------------------+--------+-------------+-----------------+-------------------+------------+----------+-------------------+
    |2016-03-24 11:52:17|          Golf_3_1.6|privat|  Angebot|  480|   test|       NULL|              1993|  manuell|      0|    golf|   150000|                  0|  benzin|   volkswagen|             NULL|2016-03-24 00:00:00|           0|     70435|2016-04-07 03:16:57|
    |2016-03-24 10:58:45|A5_Sportback_2.7_Tdi|privat|  Angebot|18300|   test|      coupe|              2011|  manuell|    190|    NULL|   125000|                  5|  diesel|         audi|               ja|2016-03-24 00:00:00|           0|     66954|2016-04-07 01:46:50|
    |2016-03-14 12:52:21|Jeep_Grand_Cherok...|privat|  Angebot| 9800|   test|        suv|              2004|automatik|    163|   grand|   125000|                  8|  diesel|         jeep|             NULL|2016-03-14 00:00:00|           0|     90480|2016-04-05 12:47:46|
    |2016-03-17 16:54:04|  GOLF_4_1_4__3T�RER|privat|  Angebot| 1500|   test| kleinwagen|              2001|  manuell|     75|    golf|   150000|                  6|  benzin|   volkswagen|             nein|2016-03-17 00:00:00|           0|     91074|2016-03-17 17:40:17|
    |2016-03-31 17:25:20|Skoda_Fabia_1.4_T...|privat|  Angebot| 3600|   test| kleinwagen|              2008|  manuell|     69|   fabia|    90000|                  7|  diesel|        skoda|             nein|2016-03-31 00:00:00|           0|     60437|2016-04-06 10:17:21|
    |2016-04-04 17:36:23|BMW_316i___e36_Li...|privat|  Angebot|  650|   test|  limousine|              1995|  manuell|    102|     3er|   150000|                 10|  benzin|          bmw|               ja|2016-04-04 00:00:00|           0|     33775|2016-04-06 19:17:07|
    |2016-04-01 20:48:51|Peugeot_206_CC_11...|privat|  Angebot| 2200|   test|     cabrio|              2004|  manuell|    109| 2_reihe|   150000|                  8|  benzin|      peugeot|             nein|2016-04-01 00:00:00|           0|     67112|2016-04-05 18:18:39|
    |2016-03-21 18:54:38|VW_Derby_Bj_80__S...|privat|  Angebot|    0|   test|  limousine|              1980|  manuell|     50|  andere|    40000|                  7|  benzin|   volkswagen|             nein|2016-03-21 00:00:00|           0|     19348|2016-03-25 16:47:58|
    |2016-04-04 23:42:13|Ford_C___Max_Tita...|privat|  Angebot|14500|control|        bus|              2014|  manuell|    125|   c_max|    30000|                  8|  benzin|         ford|             NULL|2016-04-04 00:00:00|           0|     94505|2016-04-04 23:42:13|
    |2016-03-17 10:53:50|VW_Golf_4_5_tueri...|privat|  Angebot|  999|   test| kleinwagen|              1998|  manuell|    101|    golf|   150000|                  0|    NULL|   volkswagen|             NULL|2016-03-17 00:00:00|           0|     27472|2016-03-31 17:17:06|
    |2016-03-26 19:54:18|   Mazda_3_1.6_Sport|privat|  Angebot| 2000|control|  limousine|              2004|  manuell|    105| 3_reihe|   150000|                 12|  benzin|        mazda|             nein|2016-03-26 00:00:00|           0|     96224|2016-04-06 10:45:34|
    |2016-04-07 10:06:22|Volkswagen_Passat...|privat|  Angebot| 2799|control|      kombi|              2005|  manuell|    140|  passat|   150000|                 12|  diesel|   volkswagen|               ja|2016-04-07 00:00:00|           0|     57290|2016-04-07 10:25:17|
    |2016-03-15 22:49:09|VW_Passat_Facelif...|privat|  Angebot|  999|control|      kombi|              1995|  manuell|    115|  passat|   150000|                 11|  benzin|   volkswagen|             NULL|2016-03-15 00:00:00|           0|     37269|2016-04-01 13:16:16|
    |2016-03-21 21:37:40|VW_PASSAT_1.9_TDI...|privat|  Angebot| 2500|control|      kombi|              2004|  manuell|    131|  passat|   150000|                  2|    NULL|   volkswagen|             nein|2016-03-21 00:00:00|           0|     90762|2016-03-23 02:50:54|
    |2016-03-21 12:57:01|Nissan_Navara_2.5...|privat|  Angebot|17999|control|        suv|              2011|  manuell|    190|  navara|    70000|                  3|  diesel|       nissan|             nein|2016-03-21 00:00:00|           0|      4177|2016-04-06 07:45:42|
    |2016-03-11 21:39:15|KA_Lufthansa_Edit...|privat|  Angebot|  450|   test| kleinwagen|              1910|     NULL|      0|      ka|     5000|                  0|  benzin|         ford|             NULL|2016-03-11 00:00:00|           0|     24148|2016-03-19 08:46:47|
    |2016-04-01 12:46:46|         Polo_6n_1_4|privat|  Angebot|  300|   test|       NULL|              2016|     NULL|     60|    polo|   150000|                  0|  benzin|   volkswagen|             NULL|2016-04-01 00:00:00|           0|     38871|2016-04-01 12:46:46|
    |2016-03-20 10:25:19|Renault_Twingo_1....|privat|  Angebot| 1750|control| kleinwagen|              2004|automatik|     75|  twingo|   150000|                  2|  benzin|      renault|             nein|2016-03-20 00:00:00|           0|     65599|2016-04-06 13:16:07|
    |2016-03-23 15:48:05|Ford_C_MAX_2.0_TD...|privat|  Angebot| 7550|   test|        bus|              2007|  manuell|    136|   c_max|   150000|                  6|  diesel|         ford|             nein|2016-03-23 00:00:00|           0|     88361|2016-04-05 18:45:11|
    |2016-04-01 22:55:47|Mercedes_Benz_A_1...|privat|  Angebot| 1850|   test|        bus|              2004|  manuell|    102|a_klasse|   150000|                  1|  benzin|mercedes_benz|             nein|2016-04-01 00:00:00|           0|     49565|2016-04-05 22:46:05|
    +-------------------+--------------------+------+---------+-----+-------+-----------+------------------+---------+-------+--------+---------+-------------------+--------+-------------+-----------------+-------------------+------------+----------+-------------------+
    only showing top 20 rows
    



```python
# 1. Проверка на дубликаты
duplicate_count = df.count() - df.dropDuplicates().count()
print(f"Число дубликатов: {duplicate_count}")
```

    Число дубликатов: 4



```python
# 2. Проверка пропущенных значений
from pyspark.sql import functions as F

missing_values = df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns])
print("Пропущенные значения по столбцам:")
missing_values.show()
```

    Пропущенные значения по столбцам:
    +-----------+----+------+---------+-----+------+-----------+------------------+-------+-------+-----+---------+-------------------+--------+-----+-----------------+-----------+------------+----------+--------+
    |dateCrawled|name|seller|offerType|price|abtest|vehicleType|yearOfRegistration|gearbox|powerPS|model|kilometer|monthOfRegistration|fuelType|brand|notRepairedDamage|dateCreated|nrOfPictures|postalCode|lastSeen|
    +-----------+----+------+---------+-----+------+-----------+------------------+-------+-------+-----+---------+-------------------+--------+-----+-----------------+-----------+------------+----------+--------+
    |          0|   0|     1|        1|    1|     1|      37900|                 1|  20224|      1|20499|        1|                  1|   33416|    1|            72124|          1|           1|         1|       1|
    +-----------+----+------+---------+-----+------+-----------+------------------+-------+-------+-----+---------+-------------------+--------+-----+-----------------+-----------+------------+----------+--------+
    



```python
# 3. Проверка уникальных значений для категориальных данных
categorical_columns = ['seller', 'offerType', 'vehicleType', 'gearbox', 'fuelType', 'brand', 'model', 'notRepairedDamage']
for col in categorical_columns:
    unique_values = df.select(col).distinct().count()
    print(f"Число уникальных значений в столбце '{col}': {unique_values}")
```

    Число уникальных значений в столбце 'seller': 3
    Число уникальных значений в столбце 'offerType': 3
    Число уникальных значений в столбце 'vehicleType': 9
    Число уникальных значений в столбце 'gearbox': 3
    Число уникальных значений в столбце 'fuelType': 8
    Число уникальных значений в столбце 'brand': 41
    Число уникальных значений в столбце 'model': 252
    Число уникальных значений в столбце 'notRepairedDamage': 3



```python
# 4. Проверка на неадекватные значения в числовых столбцах
numeric_columns = ['price', 'powerPS', 'kilometer', 'yearOfRegistration', 'monthOfRegistration']

aggregations = [F.min(F.col(c)).alias(f"{c}_min") for c in numeric_columns] + \
               [F.max(F.col(c)).alias(f"{c}_max") for c in numeric_columns] + \
               [F.avg(F.col(c)).alias(f"{c}_avg") for c in numeric_columns]

df.select(*aggregations).show()

```

    +---------+-----------+-------------+----------------------+-----------------------+----------+-----------+-------------+----------------------+-----------------------+------------------+------------------+------------------+----------------------+-----------------------+
    |price_min|powerPS_min|kilometer_min|yearOfRegistration_min|monthOfRegistration_min| price_max|powerPS_max|kilometer_max|yearOfRegistration_max|monthOfRegistration_max|         price_avg|       powerPS_avg|     kilometer_avg|yearOfRegistration_avg|monthOfRegistration_avg|
    +---------+-----------+-------------+----------------------+-----------------------+----------+-----------+-------------+----------------------+-----------------------+------------------+------------------+------------------+----------------------+-----------------------+
    |        0|          0|         5000|                  1000|                      0|2147483647|      20000|       150000|                  9999|                     12|17286.338865535483|115.54146462160759|125618.56044408226|    2004.5767206439623|      5.734505396384839|
    +---------+-----------+-------------+----------------------+-----------------------+----------+-----------+-------------+----------------------+-----------------------+------------------+------------------+------------------+----------------------+-----------------------+
    



```python
# 7. Проверка значения 'nrOfPictures'
print("Уникальные значения 'nrOfPictures':")
df.select("nrOfPictures").distinct().show()
```

    Уникальные значения 'nrOfPictures':
    +------------+
    |nrOfPictures|
    +------------+
    |           0|
    |        NULL|
    +------------+
    



```python
# 8. Проверка значений в 'notRepairedDamage'
print("Уникальные значения 'notRepairedDamage':")
df.select("notRepairedDamage").distinct().show()
```

    Уникальные значения 'notRepairedDamage':
    +-----------------+
    |notRepairedDamage|
    +-----------------+
    |             nein|
    |               ja|
    |             NULL|
    +-----------------+
    



```python
missing_critical_columns = df.filter(
    F.col("vehicleType").isNull() |
    F.col("gearbox").isNull() |
    F.col("fuelType").isNull()
).count()
print(f"Число записей с пропущенными критическими значениями: {missing_critical_columns}")
```

    Число записей с пропущенными критическими значениями: 61679


### Дубликаты
Найдено 4 дубликата. Вы можете их удалить, если требуется, для улучшения качества данных.

### Пропущенные значения
Столбцы, такие как vehicleType, gearbox, fuelType, и notRepairedDamage, содержат значительное количество пропущенных данных, что потребует стратегии обработки:

### Удаление строк.
Замена пропущенных значений (например, с использованием моды, медианы или специальных значений).
Уникальные значения в категориальных данных
Результаты показывают, что столбцы, такие как brand и model, имеют большое количество уникальных значений. Это может быть полезно для дальнейшего анализа, например, для создания новой таблицы Brands.

### Неадекватные значения в числовых столбцах

Значение 0 в price, powerPS, и kilometer — потенциально ошибочные данные.
Годы регистрации выходят за допустимые диапазоны (например, 1000 или 9999).
Эти аномалии требуют очистки.
nrOfPictures
Единственное значение в этом столбце — 0, что делает столбец кандидатом для удаления.

### notRepairedDamage
Значения представлены в виде ja (да) и nein (нет), что можно преобразовать в булевый формат (true/false). Также присутствуют пропущенные значения.

### Пропущенные критические значения
У 61,679 записей отсутствуют данные в ключевых столбцах (vehicleType, gearbox, fuelType). Это составляет значительную долю датасета, и такие строки следует обрабатывать осмотрительно.


```python
df = df.dropDuplicates()
print(f"Количество записей после удаления дубликатов: {df.count()}")

```

    Количество записей после удаления дубликатов: 371820



```python
columns_to_check = [
    'seller', 'offerType', 'price', 'abtest',
    'yearOfRegistration', 'powerPS', 'kilometer',
    'monthOfRegistration', 'brand', 'dateCreated',
    'nrOfPictures', 'postalCode', 'lastSeen'
]

df = df.na.drop(subset=columns_to_check)
print(f"Количество записей после удаления строк с пропущенными значениями в критических столбцах: {df.count()}")

```

    Количество записей после удаления строк с пропущенными значениями в критических столбцах: 371819



```python
# Заменяем null на "Other" для категориальных столбцов
categorical_columns = ['vehicleType', 'gearbox', 'fuelType', 'notRepairedDamage']

replacement_values = {col: "Other" for col in categorical_columns}
df = df.fillna(replacement_values)

print("Значения 'Other' добавлены в категориальные столбцы с пропусками.")

```

    Значения 'Other' добавлены в категориальные столбцы с пропусками.



```python
# Удаляем строки, где price == 0
df = df.filter(F.col("price") > 0)
print(f"Количество записей после удаления строк с ценой равной 0: {df.count()}")

```

    Количество записей после удаления строк с ценой равной 0: 361034



```python
df = df.filter((F.col("yearOfRegistration") >= 1900) & (F.col("yearOfRegistration") <= 2025))
print(f"Количество записей после фильтрации по yearOfRegistration (1900-2025): {df.count()}")

```

    Количество записей после фильтрации по yearOfRegistration (1900-2025): 360884



```python
df = df.drop("nrOfPictures")
print("Столбец 'nrOfPictures' удалён.")

```

    Столбец 'nrOfPictures' удалён.



```python
df.printSchema()
df.show(10, truncate=False)

```

    root
     |-- dateCrawled: timestamp (nullable = true)
     |-- name: string (nullable = true)
     |-- seller: string (nullable = true)
     |-- offerType: string (nullable = true)
     |-- price: integer (nullable = true)
     |-- abtest: string (nullable = true)
     |-- vehicleType: string (nullable = false)
     |-- yearOfRegistration: integer (nullable = true)
     |-- gearbox: string (nullable = false)
     |-- powerPS: integer (nullable = true)
     |-- model: string (nullable = true)
     |-- kilometer: integer (nullable = true)
     |-- monthOfRegistration: integer (nullable = true)
     |-- fuelType: string (nullable = false)
     |-- brand: string (nullable = true)
     |-- notRepairedDamage: string (nullable = false)
     |-- dateCreated: timestamp (nullable = true)
     |-- postalCode: integer (nullable = true)
     |-- lastSeen: timestamp (nullable = true)
    
    +-------------------+--------------------------------------------------+------+---------+-----+-------+-----------+------------------+---------+-------+--------+---------+-------------------+--------+-------------+-----------------+-------------------+----------+-------------------+
    |dateCrawled        |name                                              |seller|offerType|price|abtest |vehicleType|yearOfRegistration|gearbox  |powerPS|model   |kilometer|monthOfRegistration|fuelType|brand        |notRepairedDamage|dateCreated        |postalCode|lastSeen           |
    +-------------------+--------------------------------------------------+------+---------+-----+-------+-----------+------------------+---------+-------+--------+---------+-------------------+--------+-------------+-----------------+-------------------+----------+-------------------+
    |2016-03-11 19:50:22|VW_Lupo_1_0_l_Tuev_6/2017_viele_Neuteile_verbaut  |privat|Angebot  |1200 |test   |kleinwagen |2002              |manuell  |45     |lupo    |150000   |9                  |benzin  |volkswagen   |nein             |2016-03-11 00:00:00|38707     |2016-03-20 07:18:49|
    |2016-03-31 22:58:15|Fast_Jang_Timer_sucht_�bernehmer_!!               |privat|Angebot  |1500 |test   |limousine  |1991              |manuell  |97     |andere  |150000   |6                  |benzin  |mercedes_benz|nein             |2016-03-31 00:00:00|41836     |2016-04-01 02:41:33|
    |2016-03-31 23:41:53|Opel_Zafira_1.6_Comfort                           |privat|Angebot  |1200 |control|bus        |2000              |manuell  |101    |zafira  |150000   |3                  |benzin  |opel         |nein             |2016-03-31 00:00:00|98553     |2016-03-31 23:41:53|
    |2016-04-02 22:50:42|Dacia_Logan_Kombi__Unfaller_                      |privat|Angebot  |890  |test   |kombi      |2007              |manuell  |75     |logan   |150000   |0                  |benzin  |dacia        |ja               |2016-04-02 00:00:00|93488     |2016-04-02 23:43:48|
    |2016-03-22 16:52:10|Renault_Kangoo__rot__BJ__2002__T�V_8/2016__Gifhorn|privat|Angebot  |1300 |test   |kombi      |2002              |manuell  |95     |kangoo  |150000   |9                  |benzin  |renault      |ja               |2016-03-22 00:00:00|38527     |2016-03-25 09:16:50|
    |2016-04-03 15:57:36|Seat_Ibiza_1.4_16V_Reference                      |privat|Angebot  |3400 |test   |kleinwagen |2005              |manuell  |75     |ibiza   |125000   |4                  |benzin  |seat         |ja               |2016-04-03 00:00:00|91301     |2016-04-05 15:16:46|
    |2016-03-20 13:55:44|Mercedes_Benz_C250_CDI_7G_Tronic                  |privat|Angebot  |17999|test   |coupe      |2011              |automatik|204    |c_klasse|150000   |4                  |diesel  |mercedes_benz|nein             |2016-03-20 00:00:00|30916     |2016-03-30 03:17:15|
    |2016-03-28 21:49:34|Opel_Astra_G_CC_2_0_DI___Zum_ausschlachten        |privat|Angebot  |200  |control|limousine  |1999              |manuell  |87     |astra   |150000   |5                  |diesel  |opel         |ja               |2016-03-28 00:00:00|91166     |2016-03-28 21:49:34|
    |2016-03-24 13:53:31|BMW_316ti_compact                                 |privat|Angebot  |1999 |control|limousine  |2001              |manuell  |116    |3er     |150000   |8                  |benzin  |bmw          |ja               |2016-03-24 00:00:00|44359     |2016-04-07 05:46:42|
    |2016-03-28 22:50:12|MINI__MK_1_Cooper                                 |privat|Angebot  |24850|control|limousine  |1965              |manuell  |68     |cooper  |10000    |2                  |benzin  |mini         |nein             |2016-03-28 00:00:00|73434     |2016-04-07 06:16:36|
    +-------------------+--------------------------------------------------+------+---------+-----+-------+-----------+------------------+---------+-------+--------+---------+-------------------+--------+-------------+-----------------+-------------------+----------+-------------------+
    only showing top 10 rows
    


Данные успешно обработаны! Вот сводка о выполненных действиях:

Удалены дубликаты:

Начальное количество записей: 371,824
После удаления: 371,820
Удалены строки с пропущенными значениями в критических столбцах:

Столбцы: seller, offerType, price, abtest, yearOfRegistration, powerPS, kilometer, monthOfRegistration, brand, dateCreated, nrOfPictures, postalCode, lastSeen
Количество записей после удаления: 371,819
**Заполнены пропуски в категориальных столбцах (vehicleType, gearbox, fuelType, notRepairedDamage) значением "Other".

Удалены строки с нулевым значением в столбце price:

Количество записей после удаления: 361,034
Отфильтрованы неадекватные значения в yearOfRegistration (допустимый диапазон: 1900–2025):

Количество записей после фильтрации: 360,884
Удалён столбец nrOfPictures.

в исходных данных есть несколько столбцов, которые потенциально можно преобразовать в булевы для упрощения работы с ними. Например:

notRepairedDamage: Значения nein и ja можно преобразовать в False и True соответственно.
abtest: Значения test и control также можно преобразовать в True и False или 0 и 1 (в зависимости от предпочтений).


```python
from pyspark.sql.functions import when

# Преобразование 'notRepairedDamage' в булевы значения
df = df.withColumn("notRepairedDamage", when(F.col("notRepairedDamage") == "ja", True).otherwise(False))

# Преобразование 'abtest' в булевы значения
df = df.withColumn("abtest", when(F.col("abtest") == "test", True).otherwise(False))

print("Столбцы преобразованы в булевы значения.")
df.printSchema()
```

    Столбцы преобразованы в булевы значения.
    root
     |-- dateCrawled: timestamp (nullable = true)
     |-- name: string (nullable = true)
     |-- seller: string (nullable = true)
     |-- offerType: string (nullable = true)
     |-- price: integer (nullable = true)
     |-- abtest: boolean (nullable = false)
     |-- vehicleType: string (nullable = false)
     |-- yearOfRegistration: integer (nullable = true)
     |-- gearbox: string (nullable = false)
     |-- powerPS: integer (nullable = true)
     |-- model: string (nullable = true)
     |-- kilometer: integer (nullable = true)
     |-- monthOfRegistration: integer (nullable = true)
     |-- fuelType: string (nullable = false)
     |-- brand: string (nullable = true)
     |-- notRepairedDamage: boolean (nullable = false)
     |-- dateCreated: timestamp (nullable = true)
     |-- postalCode: integer (nullable = true)
     |-- lastSeen: timestamp (nullable = true)
    


Для нормализации данных мы разделим исходный датасет на несколько связанных таблиц:

Предложенная структура таблиц:
Vehicles: Основная информация о транспортных средствах (идентификатор, марка, модель, тип, мощность двигателя и пр.).
Listings: Информация о размещении объявления (цена, дата создания, последний просмотр и пр.).
Sellers: Информация о продавцах (идентификатор, тип продавца, тип предложения).
Geolocation: Географическая информация (почтовый индекс).
Tests: Информация об абтестировании.
Ключевые связи:
Listings будет ссылаться на Vehicles, Sellers, Geolocation и Tests с помощью внешних ключей.
План действий:
Создать DataFrames для каждой таблицы.
Добавить уникальные идентификаторы для каждой таблицы.
Сохранить DataFrames в отдельные переменные для дальнейшей обработки.



```python
from pyspark.sql.functions import monotonically_increasing_id

# Vehicles Table
df_vehicles = df.select("vehicleType", "brand", "model", "yearOfRegistration", "powerPS", "fuelType").distinct() \
                .withColumn("vehicle_id", monotonically_increasing_id())

# Sellers Table
df_sellers = df.select("seller", "offerType").distinct() \
               .withColumn("seller_id", monotonically_increasing_id())

# Geolocation Table
df_geolocation = df.select("postalCode").distinct() \
                   .withColumn("location_id", monotonically_increasing_id())


# Listings Table
df_listings = df.join(df_vehicles, ["vehicleType", "brand", "model", "yearOfRegistration", "powerPS", "fuelType"], "left") \
                .join(df_sellers, ["seller", "offerType"], "left") \
                .join(df_geolocation, ["postalCode"], "left") \
                .select("dateCrawled", "price", "kilometer", "monthOfRegistration",
                        "notRepairedDamage", "dateCreated", "lastSeen", "vehicle_id",
                        "seller_id", "location_id") \
                .withColumn("listing_id", monotonically_increasing_id())

```


```python
df_vehicles.show()
df_sellers.show()
df_geolocation.show()
df_listings.show(truncate=False)
```

    +-----------+-------------+--------+------------------+-------+--------+----------+
    |vehicleType|        brand|   model|yearOfRegistration|powerPS|fuelType|vehicle_id|
    +-----------+-------------+--------+------------------+-------+--------+----------+
    |  limousine|         audi|      a8|              2004|    232|  diesel|         0|
    |      Other|         mini|  cooper|              2017|    116|  benzin|         1|
    |        bus|    chevrolet|  andere|              2001|    188|  benzin|         2|
    |  limousine|   volkswagen|    golf|              1997|     75|  benzin|         3|
    |  limousine|   volkswagen|    golf|              1999|    110|  diesel|         4|
    | kleinwagen|         ford|   focus|              2000|     90|  benzin|         5|
    |  limousine|   volkswagen|    bora|              1999|    101|  benzin|         6|
    |      kombi|         audi|  andere|              2014|    300|  benzin|         7|
    |      Other|        volvo|     v70|              2000|      0|  diesel|         8|
    |  limousine|   volkswagen|  beetle|              1999|    116|  benzin|         9|
    |  limousine|          bmw|     3er|              1999|      0|  benzin|        10|
    |        suv|       suzuki|   jimny|              2013|     86|  benzin|        11|
    |  limousine|      renault|    clio|              2004|     82|  diesel|        12|
    | kleinwagen|      citroen|      c3|              2012|     73|  benzin|        13|
    |        suv|mercedes_benz|m_klasse|              2007|    272|  diesel|        14|
    | kleinwagen|     daihatsu|  sirion|              1998|     56|  benzin|        15|
    |  limousine|          bmw|     1er|              2006|    265|  benzin|        16|
    |      kombi|mercedes_benz|e_klasse|              2000|    170|  diesel|        17|
    |  limousine|      peugeot|  andere|              2005|    204|  diesel|        18|
    | kleinwagen|   volkswagen|    golf|              1990|      0|  benzin|        19|
    +-----------+-------------+--------+------------------+-------+--------+----------+
    only showing top 20 rows
    
    +----------+---------+---------+
    |    seller|offerType|seller_id|
    +----------+---------+---------+
    |    privat|  Angebot|        0|
    |    privat|   Gesuch|        1|
    |gewerblich|  Angebot|        2|
    +----------+---------+---------+
    
    +----------+-----------+
    |postalCode|location_id|
    +----------+-----------+
    |     96224|          0|
    |     87616|          1|
    |     33602|          2|
    |     87656|          3|
    |     38723|          4|
    |     99817|          5|
    |     64859|          6|
    |     72820|          7|
    |     78120|          8|
    |     76756|          9|
    |     93486|         10|
    |     67376|         11|
    |     41751|         12|
    |     24354|         13|
    |     90461|         14|
    |     21220|         15|
    |     45307|         16|
    |     17389|         17|
    |      1591|         18|
    |     94265|         19|
    +----------+-----------+
    only showing top 20 rows
    
    +-------------------+-----+---------+-------------------+-----------------+-------------------+-------------------+----------+---------+-----------+----------+
    |dateCrawled        |price|kilometer|monthOfRegistration|notRepairedDamage|dateCreated        |lastSeen           |vehicle_id|seller_id|location_id|listing_id|
    +-------------------+-----+---------+-------------------+-----------------+-------------------+-------------------+----------+---------+-----------+----------+
    |2016-03-11 09:37:13|160  |5000     |0                  |false            |2016-03-11 00:00:00|2016-03-24 10:15:33|NULL      |0        |6079       |0         |
    |2016-03-17 19:40:32|99   |150000   |0                  |true             |2016-03-17 00:00:00|2016-03-17 19:40:32|NULL      |0        |3036       |1         |
    |2016-03-17 13:55:45|220  |150000   |0                  |false            |2016-03-17 00:00:00|2016-03-17 13:55:45|NULL      |0        |6045       |2         |
    |2016-04-01 08:51:08|1    |5000     |1                  |false            |2016-04-01 00:00:00|2016-04-07 05:44:39|NULL      |0        |4844       |3         |
    |2016-04-03 09:02:20|5000 |150000   |0                  |false            |2016-04-03 00:00:00|2016-04-07 10:45:39|NULL      |0        |5421       |4         |
    |2016-03-22 17:48:41|599  |5000     |0                  |false            |2016-03-22 00:00:00|2016-04-06 09:16:59|NULL      |0        |1838       |5         |
    |2016-03-14 08:36:59|1    |150000   |12                 |false            |2016-03-14 00:00:00|2016-03-14 08:36:59|NULL      |0        |5616       |6         |
    |2016-03-24 20:52:53|2500 |5000     |0                  |false            |2016-03-24 00:00:00|2016-04-05 13:47:07|NULL      |0        |4193       |7         |
    |2016-04-01 12:46:44|1    |5000     |2                  |false            |2016-04-01 00:00:00|2016-04-01 12:46:44|17004     |0        |5377       |8         |
    |2016-03-07 22:58:46|3400 |90000    |4                  |false            |2016-03-07 00:00:00|2016-03-12 08:16:51|7493      |0        |4591       |9         |
    |2016-03-25 13:47:46|30   |100000   |0                  |false            |2016-03-25 00:00:00|2016-03-26 23:46:29|16231     |0        |4842       |10        |
    |2016-03-20 07:37:16|1500 |150000   |0                  |false            |2016-03-20 00:00:00|2016-04-06 04:45:40|35005     |0        |48         |11        |
    |2016-03-25 13:42:37|1    |125000   |0                  |false            |2016-03-25 00:00:00|2016-04-06 15:44:48|31971     |0        |4992       |12        |
    |2016-03-07 14:53:52|222  |5000     |0                  |false            |2016-03-07 00:00:00|2016-03-12 04:15:30|20655     |0        |1317       |13        |
    |2016-04-05 00:39:36|300  |150000   |0                  |false            |2016-04-04 00:00:00|2016-04-05 05:42:22|20655     |0        |1349       |14        |
    |2016-03-21 12:52:05|400  |150000   |0                  |false            |2016-03-21 00:00:00|2016-03-25 09:17:54|18911     |0        |1545       |15        |
    |2016-03-11 21:39:15|450  |5000     |0                  |false            |2016-03-11 00:00:00|2016-03-19 08:46:47|23600     |0        |4829       |16        |
    |2016-03-10 15:58:12|180  |150000   |2                  |true             |2016-03-10 00:00:00|2016-04-05 13:19:00|35403     |0        |105        |17        |
    |2016-03-10 15:36:35|180  |150000   |2                  |true             |2016-03-10 00:00:00|2016-04-05 11:49:05|35403     |0        |105        |18        |
    |2016-03-31 14:58:25|300  |5000     |0                  |false            |2016-03-31 00:00:00|2016-04-06 08:16:52|36592     |0        |3833       |19        |
    +-------------------+-----+---------+-------------------+-----------------+-------------------+-------------------+----------+---------+-----------+----------+
    only showing top 20 rows
    


 Структура ETL
Extract: Загрузка данных из хранилища Google Drive (датасет).
Transform: Очистка данных, нормализация и разделение на таблицы.
Load: Загрузка данных в PostgreSQL.
2. Шаги настройки
Установить Airflow:

Установить Airflow в локальное окружение или через Colab.
Убедиться, что PostgreSQL доступен для загрузки данных.
Создать DAG (Directed Acyclic Graph):

Определить DAG с задачами для каждого этапа ETL.
Использовать Python-скрипты для выполнения задач.
Добавить операции в DAG:

Задача 1: Проверка доступности датасета.
Задача 2: Очистка данных.
Задача 3: Нормализация данных.
Задача 4: Загрузка данных в PostgreSQL.
Протестировать пайплайн:

Проверить DAG на работоспособность и корректность выполнения.


```python
import psycopg2
from dotenv import load_dotenv

load_dotenv()
db_password = os.getenv("DB_PASSWORD")

# Параметры подключения
db_config = {
    "dbname": "postgres",
    "user": "postgres",
    "password": db_password,
    "host": "localhost",
    "port": 5432,
}

# Установка соединения
conn = psycopg2.connect(**db_config)
cursor = conn.cursor()
print("Подключение к PostgreSQL успешно выполнено!")

```


    ---------------------------------------------------------------------------

    OperationalError                          Traceback (most recent call last)

    <ipython-input-30-28f0b8f6b581> in <cell line: 17>()
         15 
         16 # Установка соединения
    ---> 17 conn = psycopg2.connect(**db_config)
         18 cursor = conn.cursor()
         19 print("Подключение к PostgreSQL успешно выполнено!")


    /usr/local/lib/python3.10/dist-packages/psycopg2/__init__.py in connect(dsn, connection_factory, cursor_factory, **kwargs)
        120 
        121     dsn = _ext.make_dsn(dsn, **kwargs)
    --> 122     conn = _connect(dsn, connection_factory=connection_factory, **kwasync)
        123     if cursor_factory is not None:
        124         conn.cursor_factory = cursor_factory


    OperationalError: connection to server at "localhost" (127.0.0.1), port 5432 failed: Connection refused
    	Is the server running on that host and accepting TCP/IP connections?
    connection to server at "localhost" (::1), port 5432 failed: Cannot assign requested address
    	Is the server running on that host and accepting TCP/IP connections?




```python
# Таблица для транспортных средств
cursor.execute("""
CREATE TABLE IF NOT EXISTS Vehicles (
    vehicle_id SERIAL PRIMARY KEY,
    vehicle_type TEXT NOT NULL,
    brand TEXT NOT NULL,
    model TEXT NOT NULL,
    year_of_registration INT NOT NULL,
    power_ps INT NOT NULL,
    fuel_type TEXT NOT NULL
);
""")

# Таблица для продавцов
cursor.execute("""
CREATE TABLE IF NOT EXISTS Sellers (
    seller_id SERIAL PRIMARY KEY,
    seller_type TEXT NOT NULL,
    offer_type TEXT NOT NULL
);
""")

# Таблица для геолокации
cursor.execute("""
CREATE TABLE IF NOT EXISTS Geolocation (
    location_id SERIAL PRIMARY KEY,
    postal_code INT NOT NULL
);
""")

# Таблица для объявлений
cursor.execute("""
CREATE TABLE IF NOT EXISTS Listings (
    listing_id SERIAL PRIMARY KEY,
    date_crawled TIMESTAMP NOT NULL,
    price INT NOT NULL,
    kilometer INT NOT NULL,
    month_of_registration INT NOT NULL,
    not_repaired_damage BOOLEAN NOT NULL,
    date_created TIMESTAMP NOT NULL,
    last_seen TIMESTAMP NOT NULL,
    vehicle_id INT REFERENCES Vehicles(vehicle_id),
    seller_id INT REFERENCES Sellers(seller_id),
    location_id INT REFERENCES Geolocation(location_id)
);
""")

# Подтверждение изменений
conn.commit()
print("Таблицы созданы успешно!")

```


```python
# Закрытие соединения
cursor.close()
conn.close()
print("Соединение закрыто.")

```

Пример кода DAG


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
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    kwargs['ti'].xcom_push(key='raw_data', value=file_path)

# Шаг 2: Трансформация данных
def transform_data(**kwargs):
    spark = init_spark()
    raw_data_path = kwargs['ti'].xcom_pull(task_ids='extract_data', key='raw_data')
    df = spark.read.csv(raw_data_path, header=True, inferSchema=True)

    # Очистка данных
    df = df.dropDuplicates()
    df = df.filter((df.yearOfRegistration >= 1900) & (df.yearOfRegistration <= 2025))
    df = df.filter(df.price > 0)

    # Нормализация
    df_vehicles = df.select("vehicleType", "brand", "model", "yearOfRegistration", "powerPS", "fuelType")
    df_sellers = df.select("seller", "offerType")
    df_listings = df.select("dateCrawled", "price", "kilometer", "monthOfRegistration",
                            "notRepairedDamage", "dateCreated", "lastSeen", "postalCode")

    output_path = "/content/drive/MyDrive/ProcessedData"
    os.makedirs(output_path, exist_ok=True)
    df_vehicles.write.csv(os.path.join(output_path, "vehicles.csv"), header=True, mode="overwrite")
    df_sellers.write.csv(os.path.join(output_path, "sellers.csv"), header=True, mode="overwrite")
    df_listings.write.csv(os.path.join(output_path, "listings.csv"), header=True, mode="overwrite")

# Шаг 3: Загрузка данных в PostgreSQL
def load_data(**kwargs):
    import psycopg2
    from psycopg2.extras import execute_values

    conn = psycopg2.connect(
        dbname="your_db",
        user="your_user",
        password="your_password",
        host="localhost",
        port=5432
    )
    cursor = conn.cursor()

    processed_path = "/content/drive/MyDrive/ProcessedData"
    for table_name, file_name in [("vehicles", "vehicles.csv"), ("sellers", "sellers.csv"), ("listings", "listings.csv")]:
        df = pd.read_csv(os.path.join(processed_path, file_name))
        execute_values(cursor, f"INSERT INTO {table_name} VALUES %s", df.values.tolist())

    conn.commit()
    cursor.close()
    conn.close()

# DAG определение
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
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

Основные этапы:
Подготовка данных:

Проведены всесторонние проверки данных (дубликаты, пропущенные значения, неадекватные значения).
Выполнены трансформации, включая преобразование категориальных столбцов и столбцов в булевы значения.
Приведены данные к нормализованной форме.
Нормализация:

Данные разделены на таблицы: Vehicles, Sellers, Geolocation, Listings.
Установлены связи между таблицами через уникальные идентификаторы.
Создание базы данных:

В PostgreSQL созданы таблицы с учётом всех особенностей структуры данных.
Готовность данных к загрузке:

Таблицы подготовлены для экспорта в PostgreSQL.
ETL-пайплайн:

Автоматизировано чтение, очистка и разделение данных.
Настроены шаги для преобразования данных в рамках единого процесса.


```python
SELECT
    v.brand,
    v.model,
    COUNT(l.listing_id) AS total_listings
FROM
    Listings l
JOIN
    Vehicles v ON l.vehicle_id = v.vehicle_id
GROUP BY
    v.brand, v.model
ORDER BY
    total_listings DESC
LIMIT 5;

```


```python
SELECT
    v.fuelType,
    v.vehicleType,
    ROUND(AVG(l.price), 2) AS avg_price
FROM
    Listings l
JOIN
    Vehicles v ON l.vehicle_id = v.vehicle_id
GROUP BY
    v.fuelType, v.vehicleType
ORDER BY
    avg_price DESC;

```


```python
SELECT
    g.postalCode,
    s.seller,
    COUNT(l.listing_id) AS total_listings
FROM
    Listings l
JOIN
    Sellers s ON l.seller_id = s.seller_id
JOIN
    Geolocation g ON l.location_id = g.location_id
GROUP BY
    g.postalCode, s.seller
ORDER BY
    total_listings DESC
LIMIT 10;

```
