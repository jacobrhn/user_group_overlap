from class_ETL import ETL

pipeline = ETL("data_specs.json")
print(pipeline.raw_data)
df_final = pipeline.run()

