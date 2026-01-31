# How to Run

## 1. Configure
Edit `app/config/config.yaml` and `scripts/config.yaml`.

---

## 2. Build and run server (Docker)

Build image & Run container:
```bash
docker build -f docker/Dockerfile -t web-counter .
docker run -p 8080:8080 web-counter
```

## 3. Run client

Activate virtual environment:

```bash
.\.venv\Scripts\activate
pip install aiohttp pyyaml
```
Run script:

```bash
python client.py
```