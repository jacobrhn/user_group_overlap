import os.path

import pandas as pd
from class_DataLoader import DataLoader

class ETL:
    df_enhanced_data = pd.DataFrame

    def __init__(self, data_specs):
        self.importer = DataLoader(data_specs)
        self.load_data()

    def load_data(self):
        self.raw_data = self.importer.load_data()
        self.raw_dev_data = self.raw_data["dev_w_rbac"]
        self.raw_repo_data = self.raw_data["repo_w_dev"]
        #self.raw_rbac_roles = self.raw_data["rbac_roles"]

    def enhance_xyz(self):
        pass

    def enhance_data(self):
        self.enhance_xyz()

    def create_enhanced_df(self):
        self.df_enhanced_data = pd.merge(self.raw_data["dev_w_rbac"], self.raw_data["repo_w_dev"], left_index=True, right_index=True)

    def save_final_df(self):
        database_path = self.importer.database_config["database_path"]
        enhanced_data_file_name = "enhanced_data.xlsx"
        self.df_enhanced_data.to_excel(os.path.join(database_path, enhanced_data_file_name))

    def run(self):
        self.enhance_data()
        self.create_enhanced_df()
        self.save_final_df()
        return self.df_enhanced_data

