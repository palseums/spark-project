order_df = spark.createDataFrame(orders_list).toDF("order_id", "prod_id", "unit_price", "qty")
product_df = spark.createDataFrame(product_list).toDF("prod_id", "prod_name", "list_price", "qty")
join_expr = order_df.prod_id == product_df.prod_id
product_renamed_df = product_df.withColumnRenamed("qty","reorder_qty")
order_df.join(product_renamed_df,join_expr,"inner")\
    .drop(product_renamed_df.prod_id)
    .select("order_id", "prod_name","unit_price","qty","prod_id")\
    .show()

