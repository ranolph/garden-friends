FROM python:3.12-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 \
    GARDEN_FRIENDS_DATA_DIR=/data ART_DIR=/app/species_art WEB_BIND=0.0.0.0 WEB_PORT=8090
COPY requirements.txt ./
RUN python -m pip install --no-cache-dir -r requirements.txt
COPY main.py garden_friends_settings.py server.py ./
COPY ARTWORK_MANIFEST.json ./
COPY static ./static
COPY species_art ./species_art
EXPOSE 8090
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8090/api/health', timeout=3).read()"
CMD ["python", "-u", "server.py"]
