# Use Python slim
FROM python:3.11-slim

# Set workdir
WORKDIR /app

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt || \
    pip install --trusted-host pypi.org --trusted-host pypi.python.org --trusted-host files.pythonhosted.org -r requirements.txt

# Copy app
COPY app.py .

# Expose port
EXPOSE 5000

# Run Flask app
CMD ["python", "app.py"]
