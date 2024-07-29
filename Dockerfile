FROM apache/airflow:2.9.3

# Switch to airflow user to install Python packages
USER airflow

# Install additional Python packages
RUN pip install --no-cache-dir \
    numpy \
    pandas \
    requests \
    minio \
    unidecode \
    psycopg2-binary \
    python-dateutil \
    sqlalchemy \
    openpyxl

# If needed, you can switch back to root for any further configuration
USER root
