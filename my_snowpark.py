# Import the Session class from the snowflake.snowpark.session module
from snowflake.snowpark.session import Session
from dotenv import load_dotenv
import os
import sys 

load_dotenv()

account = os.environ.get("account")
user = os.environ.get("user")
password = os.environ.get("password")
role = os.environ.get("role")
warehouse = os.environ.get("warehouse")
database = os.environ.get("database")
schema = os.environ.get("schema")

# Create Session object
def create_session_object():
    # Define the connection parameters
    connection_parameters = {
      "account": account,
      "user": user,
      "password": password,
      "role": role,
      "warehouse": warehouse,
      "database": database,
      "schema": schema
    }
    
    
    # Create the session object with the provided connection parameters
    session = Session.builder.configs(connection_parameters).create()
    
    # Print the contents of your session object
    # Print(session)
    
    return session 
    
    
# Function call    
create_session_object()

print(create_session_object)

# -------------------------------------------------
#                CREATE A DATAFRAME
# -------------------------------------------------

# Import Libraries 
from snowflake.snowpark.functions import col

def create_dataframe(session):
  
    # Create a dataframe
    df_table = session.table("CALL_CENTER")

    #---------------------------------
    # **ACTIONS**
    #---------------------------------

    # count method
    df_table.count()
    # print(df_table.count()) 
  
    # show method
    df_table.show()
  
    # collect method
    df_results = df_table.collect()
    # print(df_results) 
  
    #---------------------------------
    # **TRANSFORMATIONS **
    #---------------------------------
  
    df_filtered = df_table.filter(col("CC_NAME") == 'NY Metro')
  
    # Chaining method calls
    # df_filtered = df_table.filter(col("AGE") > 30).sort(col("AGE").desc()).limit(10)

    df_filtered.show()
  
    df_filtered.collect()
  
    df_filtered_persisted = df_filtered.collect()
    
    return df_filtered
    # print(df_filtered_persisted)
# ------------------------------------------------------------------------------------------------------------------
    
    # FUNCTION CALLS

# call session object
session = create_session_object()

# call create dataframe 
_ = create_dataframe(session) 

# end your session
session.close()