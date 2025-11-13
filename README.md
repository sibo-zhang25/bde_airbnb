# bde_airbnb
UTS data ELT project with Airflow and dbt Cloud

The codebase is of four parts: first part is the database setup file using a cloud provider(e.g. Google Cloud)
Second part is the automatic data injestion with Airflow(on Google Cloud) using the dag script file.All data in the zip file
are injested using this method. 
Then the data is loaded and transformed with dbt cloud with a bronze-silver-gold structure. Note that this part could be but not was not
automated with Airflow. 
The final part is the analysis part with analysis_data_mart.sql file. 
