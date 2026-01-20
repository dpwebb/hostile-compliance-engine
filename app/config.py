import os

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+psycopg://app:app@localhost:5432/app")
UPLOAD_DIR = os.getenv("UPLOAD_DIR", "./data/uploads")
