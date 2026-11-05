# ============================================
# DOCKERFILE FOR PIPELINE SERVICE
# ============================================

# Step 1: Use Python 3.11 as base image
FROM python:3.11-slim

# Step 2: Set working directory inside container
WORKDIR /app

# Step 3: Install system dependencies (required for some Python packages)
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    curl \
    openjdk-21-jre-headless \
    procps \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Step 4: Copy requirements file (we'll create this next)
COPY requirements.txt .

# Step 5: Install Python packages
RUN pip install --no-cache-dir -r requirements.txt

# Step 6: Install PySpark separately (larger package)
RUN pip install --no-cache-dir pyspark==3.5.0

# Step 6.1: Install JDBC drivers required by Spark jobs
RUN mkdir -p /opt/spark/jars \
    && curl -fL https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar \
        -o /opt/spark/jars/mysql-connector-java-8.0.33.jar \
    && curl -fL https://repo1.maven.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar \
        -o /opt/spark/jars/postgresql-42.6.0.jar

# Step 7: Copy source code
COPY src/ /app/src/
COPY scripts/ /app/scripts/
COPY tests/ /app/tests/
COPY great_expectations/ /app/great_expectations/

# Step 8: Set Python path so imports work
ENV PYTHONPATH=/app/src

# Step 9: Default command (keeps container alive)
CMD ["tail", "-f", "/dev/null"]
