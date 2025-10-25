# Image de base légère
FROM python:3.11-slim

# Répertoire de travail
WORKDIR /app

# Copier les fichiers FastAPI
COPY . /app

# Installer les dépendances
RUN pip install --no-cache-dir fastapi uvicorn requests pandas

# Exposer le port FastAPI
EXPOSE 8000

# Lancer l'application
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
