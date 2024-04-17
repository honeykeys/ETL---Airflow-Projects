# ETL + Airflow Project

These are two data engineering projects I completed to learn about the data engineering process. 


### Met API

In the first project, metapi.py, I ping the New York Metropolitan Museum of Art public api for object data. The code is hosted on an Azure Function triggered by an HTTP request, which leverages the Azure ecosystem to store the raw JSON response from the API into Azure BLOB Storage. The program then utilizes Pandas to normalize pieces of data from the response into two small tables (Artist and Object) to store in an Azure SQL Database.

I wanted to highlight my understanding of each part of the ETL (Extract, Transform, Load) process within a cloud environment. Written entirely in Python, 

### Met DAG

This is a simpler project that utilizes Airflow to ping the same Met API for collection data, then stores the response into a CSV. The DAG is triggered from my local Linux machine using a CRON job.


There are far more sophisticated means of working with ETL processes, but this project represents my initial exploration of the topic!