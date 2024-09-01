#TelematicsStream 

This project explains how telematics data is streamed from Kafka topics to S3 or delta lake using PySpark streaming

Architecture:

![Output/img.png](img.png)
Fig: Architecture 

Whole this process can be orchestrated through Airflow.

Outputs:
Idle Engine:
![Output/Screenshot 2024-09-01 224249.png](Output/Screenshot 2024-09-01 224249.png)
Speed limit exceed:
![Speed limit exceed](Output/Screenshot 2024-09-01 224338.png)
Right Turn:
![Right turns](Output/Screenshot 2024-09-01 224413.png)
Harsh breaking:
![Harsh breaking](Output/Screenshot 2024-09-01 224440.png)
Storages:
![img_1.png](img_1.png)