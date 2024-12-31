# Databricks notebook source
# MAGIC %md
# MAGIC ### MOUNTING MEANS ESTABLISING CONNECTION BETWEEN STOARGE ACCOUNT AND DELTA TABLE 

# COMMAND ----------

def check_mount(container_name,storage_account_name,application_id,tenanat_id,secret_key_value):
    
    
    mnt_point = "/mnt/"+storage_account_name+"/"+container_name
    mnt_flag = False
    for ele in dbutils.fs.mounts():
        for i in ele:
            if str(i) == str(mnt_point):
                mnt_flag= True
    
    if mnt_flag == True:
        print(f"Already mounted {container_name} container")
        
    else:
        source_val = "abfss://"+container_name+"@"+storage_account_name+".dfs.core.windows.net/"
        mnt_point_val = "/mnt/"+storage_account_name+"/"+container_name
        configs = {
            "fs.azure.account.auth.type": "OAuth",
            "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
            "fs.azure.account.oauth2.client.id": application_id,
            "fs.azure.account.oauth2.client.secret": secret_key_value,
            "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenanat_id}/oauth2/token"
        }
        try:
            dbutils.fs.mount(
                source=source_val,
                mount_point=mnt_point_val,
                extra_configs=configs
            )
        except Exception as e:
            print(f"Error mounting {container_name} container")
            return e.message;
        else:
            print(f"Successfully mounted {container_name} container")
    

# COMMAND ----------

def mount_func():
    check_mount(container_name,storage_account_name,application_id,tenanat_id,secret_key_value)
    
# COMMAND ----------

mount_func()