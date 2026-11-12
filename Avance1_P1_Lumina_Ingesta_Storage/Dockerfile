FROM python:3.11-slim

WORKDIR /app

# Copiar dependencias e instalar
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código de la plataforma Lumina (P1, P2 y P3)
COPY . .

# Cloud Run inyecta la variable de entorno PORT (por defecto 8080)
ENV PORT=8080
EXPOSE 8080

# Ejecutar el servidor de Exposición y Front-End (Entrega 3 - P3) con Gunicorn
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--workers", "2", "--threads", "4", "--timeout", "120", "08_app_exposicion_frontend_lumina:app"]
