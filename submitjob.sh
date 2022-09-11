gsutil cp  pyspark_job.py gs://nftrawdata/chapter-5/code/
gcloud dataproc jobs submit pyspark --cluster=cluster-a1c7 --region=us-central1 gs://nftrawdata/chapter-5/code/pyspark_job.py

