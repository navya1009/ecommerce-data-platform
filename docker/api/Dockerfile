# ============================================
# DOCKERFILE FOR MOCK API SERVER
# ============================================

FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/ /app/src/

# Set Python path
ENV PYTHONPATH=/app/src

# Expose port
EXPOSE 8000

# Run the API server
CMD ["uvicorn", "src.api.mock_api_server:app", "--host", "0.0.0.0", "--port", "8000"]