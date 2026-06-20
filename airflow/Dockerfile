FROM apache/airflow:3.2.2

# Copy the requirements file into the container
COPY requirements.txt /requirements.txt

# Install the additional dependencies
RUN pip install --no-cache-dir -r /requirements.txt
