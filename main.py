from class_ETL import ETL

filepath_export = "~/data/py/user_group_overlap/data_export/"

pipeline = ETL("data_specs.json", filepath_export)
print(pipeline.raw_data)
df_final = pipeline.run()

