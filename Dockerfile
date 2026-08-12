FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Aterrissa um lote datado e roda o pipeline. Requer as variaveis R2_* via env.
CMD ["sh", "-c", "python -m etl.seed_source && python main.py"]
