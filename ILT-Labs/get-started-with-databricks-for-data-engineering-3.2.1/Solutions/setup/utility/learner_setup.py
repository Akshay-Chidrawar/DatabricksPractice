# Databricks notebook source
import pandas as pd 

class LearnerSetup:

    def __init__(self, catalog_name, create_employees_csv2=False):
        """
        Initialize the Learner's environment with the necessary materials.

        Parameters:
        - catalog_name (str): The name of the catalog to be created or used for the module (course). This parameter is required and must be provided.
        """

        ## Error if a catalog_name is not specified
        if not catalog_name:
            raise ValueError("Must supply a catalog name for the module.")

        self.catalog_name = catalog_name
        self.user_name = self.get_user_name()
        self.my_schema = self.user_name         ## Schema is the user name
        self.volume_name = 'myfiles'

        ## Hidden variables to bold text
        self.__bold = '\033[1m'
        self.__reset = '\033[0m'

        ## Setup the course catalog, schema, volume and csv file(s)

        self.create_catalog()
        self.create_schema()
        self.create_volume()
        self.create_employees_csv()
        if create_employees_csv2 == True:
            self.create_employees_csv2()
        self.print_module_info()
        

    def create_catalog(self):
        """
        Creates the specified catalog in Unity Catalog if it does not already exist and adds permissions to the catalog.
        """
        ## Create the catalog if it doesn't exist
        create_catalog_qry = f'CREATE CATALOG IF NOT EXISTS {self.catalog_name};'
        spark.sql(create_catalog_qry)
            
        ##
        ## Confirm the catalog exists
        ##

        ## Store available catalogs
        catalogs = spark.sql("SHOW CATALOGS")

        # Convert the result to a DataFrame and filter based on the catalog name to confirm it exists
        catalogs_df = catalogs.toPandas()
        catalog_exists = catalogs_df.catalog.isin([self.catalog_name]).any()

        # Print the result
        if catalog_exists:
            print(f"The module's Catalog {self.__bold}{self.catalog_name}{self.__reset} is available in your workspace.")
        else:
            print(f"The module's catalog '{self.__bold}{self.catalog_name}{self.__reset}' does not exist.")

        # ## Add permissions to catalog
        # try:
        #     ## add catalog permissions
        #     print(f'Setting permissions USE CATALOG and CREATE SCHEMA on the {self.__bold}{self.catalog_name}{self.__reset} Catalog.')
        #     spark.sql(f'GRANT USAGE ON CATALOG {self.catalog_name} TO `Curriculum Team`')
        #     spark.sql(f'GRANT CREATE SCHEMA ON CATALOG {self.catalog_name} TO `Curriculum Team`')
        # except:
        #     pass
        

    def get_user_name(self):
        """
        Get the user name from the Databricks workspace.
        """
        ## Query to get the current user name 
        curr_user_qry = 'SELECT current_user() as user'

        ## Store the result as a string
        full_user_name = spark.sql(curr_user_qry).first()['user']

        ## Clean the user name by removing after the @ and replacing the . with an _
        user_name = full_user_name.split('@')[0].replace('.', '_')
        return user_name
        

    def create_schema(self):
        """
        Create a schema in the module's catalog using the user's account name.
        """
        create_schema = f'CREATE SCHEMA IF NOT EXISTS {self.catalog_name}.{self.my_schema}'
        spark.sql(create_schema)

        print(f'Your schema {self.__bold}{self.catalog_name}.{self.my_schema}{self.__reset} is available.')


    def drop_user_schema(self):
        """
        Drop the user's schema using the account name if it exists.
        """
        drop_schema_qry = f'DROP SCHEMA IF EXISTS {self.catalog_name}.{self.my_schema} CASCADE'
        spark.sql(drop_schema_qry)

        print(f'Dropped your schema {self.__bold}{self.catalog_name}.{self.my_schema}{self.__reset}.')    
    
    
    def create_volume(self):
        """
        Create a volume in the module's catalog using the specified volume_name.
        """
        ## Specify the volume name
        create_volume_location = f'{self.catalog_name}.{self.my_schema}.{self.volume_name}'

        ## Create the volume
        createVolume = f'CREATE VOLUME IF NOT EXISTS {create_volume_location}'
        spark.sql(createVolume)
        print(f'Your volume {self.__bold}{self.volume_name}{self.__reset} in {self.__bold}{self.catalog_name}.{self.my_schema}{self.__reset} is available.')


    ##
    ## Create's the CSV files for this module within the specified volume_name.
    ##
    def create_employees_csv(self):
        """
        Creates the employees.csv file in the specified catalog.Schema.Volume.
        """
        # Create data for the CSV file
        data = [
            ["1111", "Kristi", "USA", "Manager"],
            ["2222", "Sophia", "Greece", "Developer"],
            ["3333", "Peter", "USA", "Developer"],
            ["4444", "Zebi", "Pakistan", "Administrator"]
        ]
        columns = ["ID","Firstname", "Country", "Role"]

        ## Create the DataFrame
        df = pd.DataFrame(data, columns=columns)

        ## Create the CSV file in the course Catalog.Schema.Volume
        df.to_csv(f"/Volumes/{self.catalog_name}/{self.my_schema}/{self.volume_name}/employees.csv", index=False)

        print(f"The {self.__bold}employees.csv{self.__reset} was created in your schema's {self.__bold}{self.volume_name}{self.__reset} volume")


    def create_employees_csv2(self):
        """
        Creates the employees2.csv file in the specified catalog.Schema.Volume.
        """
        # Create data for the CSV file
        data = [
            [5555, 'Alex','USA', 'Instructor'],
            [6666, 'Sanjay','India', 'Instructor']
        ]
        columns = ["ID","Firstname", "Country", "Role"]

        ## Create the DataFrame
        df = pd.DataFrame(data, columns=columns)

        ## Create the CSV file in the course Catalog.Schema.Volume
        df.to_csv(f"/Volumes/{self.catalog_name}/{self.my_schema}/{self.volume_name}/employees2.csv", index=False)

        print(f"The {self.__bold}employees.csv2{self.__reset} was created in your schema's {self.__bold}{self.volume_name}{self.__reset} volume")


    def print_workflow_job_info(self):
        path = dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None)
        newpath = path.replace('DEWD00 - 04-Creating a Simple Databricks Workflow','')
        task1path = newpath + 'DEWD00 - 04A-Workflow Task 1 - Setup - Bronze'
        task2path = newpath + 'DEWD00 - 04B-Workflow Task 2 - Silver - Gold'
        
        print(f'Name your job: {self.__bold}{self.my_schema}_Example{self.__reset}')
        print(' ')
        print(f'{self.__bold}NOTEBOOK PATHS FOR TASKS{self.__reset}')
        print(f'- Task 1 notebook path: \n{self.__bold}{task1path}{self.__reset}')
        print(f'- Task 2 notebook path: \n{self.__bold}{task2path}{self.__reset}')

    def print_module_info(self):
        """
        Print the environment information for the student.
        """
        print(' ')
        print(f'{self.__bold}YOUR WORKSPACE INFORMATION:{self.__reset}')
        print(f'Module catalog: {self.__bold}{self.catalog_name}{self.__reset}')
        print(f'Module schema: {self.__bold}{self.my_schema}{self.__reset}')
        print(f'Module volume: {self.__bold}{self.volume_name}{self.__reset}')
    
    def create_taxi_files(self):
        """
        Create the samples.nyctaxi.trips Delta as a csv file in the user's volume.
        """
        spark.sql(f'CREATE VOLUME IF NOT EXISTS {self.catalog_name}.{self.my_schema}.taxi_files')
        output_volume = f'/Volumes/{self.catalog_name}/{self.my_schema}/taxi_files'
        

        sdf = spark.table("samples.nyctaxi.trips")
        
        (sdf
         .write
         .mode("overwrite")
         .csv(output_volume, header=True)
         )
        
        print(f'The taxi data is available in the {self.__bold}taxi_files{self.__reset} volume within the {self.__bold}{self.catalog_name}{self.__reset} Catalog in your schema {self.__bold}{self.my_schema}{self.__reset}.')
    
    
    def drop_taxi_volume(self):
        """
        Drop the taxidata volume if exists.
        """
        spark.sql(f'DROP VOLUME IF EXISTS {self.catalog_name}/{self.my_schema}.taxidata')
