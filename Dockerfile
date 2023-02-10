FROM public.ecr.aws/dataminded/spark-k8s-glue:v3.1.2-hadoop-3.3.1 

ENV PYSPARK_PYTHON python3

WORKDIR /app

COPY tarik_capstone.py

 