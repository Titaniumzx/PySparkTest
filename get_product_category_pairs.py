from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

def get_product_category_pairs(
    products_df: DataFrame,
    categories_df: DataFrame,
    relations_df: DataFrame
) -> DataFrame:

    products_with_relations = products_df.join(
        relations_df, 
        "product_id", 
        "left"
    )    

    result = products_with_relations.join(
        categories_df,
        "category_id",
        "left"
    )
    
    return result.select(
        "product_name",
        "category_name"
    ).orderBy("product_name", "category_name")

# Создаем Spark сессию
spark = SparkSession.builder.appName('ProductCategories').getOrCreate()

# Тестовые данные
products_data = [
    (1, "Lenovo Laptop"),
    (2, "iPhone 13"),
    (3, "Coffee Maker"),
    (4, "Wireless Headphones"),
    (5, "Smart Watch"),
    (6, "E-book Reader"),
    (7, "Fitness Tracker"),
    (8, "HP Laptop"),
    (9, "Samsung Tablet"),
    (10, "Gaming Console")
]

categories_data = [
    (101, "Electronics"),
    (102, "Home Appliances"),
    (103, "Mobile Devices"),
    (104, "Computers"),
    (105, "Accessories"),
    (106, "Gaming"),
    (107, "Kitchen Appliances")
]

relations_data = [
    (1, 101), (1, 104),
    (2, 101), (2, 103),
    (3, 102), (3, 107),
    (4, 101), (4, 105),
    (5, 101),
    (8, 101), (8, 104),
    (9, 101), (9, 103),
    (10, 106)
]

products = spark.createDataFrame(products_data, ["product_id", "product_name"])
categories = spark.createDataFrame(categories_data, ["category_id", "category_name"])
relations = spark.createDataFrame(relations_data, ["product_id", "category_id"])

result = get_product_category_pairs(products, categories, relations)
result.show(truncate=False)
