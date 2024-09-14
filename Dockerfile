FROM apache/airflow:2.8.1

# Install any additional packages
COPY requirements.txt /requirements.txt

RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir -r /requirements.txt
