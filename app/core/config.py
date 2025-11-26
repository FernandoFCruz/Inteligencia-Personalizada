import os

class Settings:
 def __init__(self):
  # Database
  self.database_url = os.getenv(
   "DATABASE_URL",
   "postgresql://postgres:1234@localhost:5432/devotum"
  )

  embed_model_path = os.getenv("MODEL_PATH","D:\Faculdade\8_fase\TCC\Inteligencia-Personalizada\Inteligencia-Personalizada\Modelos\multilingual-e5-base")
  self.embed_model_path=embed_model_path

  # Llama Server (Ollama)
  llama_server = os.getenv("LLAMA_SERVER", "http://localhost:11434")
  self.llama_server = llama_server.rstrip("/")  # remove / final para evitar erro

  # ChromaDB directory
  self.chroma_dir = os.getenv("CHROMA_DIR", "./chroma_store")
  if not os.path.exists(self.chroma_dir):
   os.makedirs(self.chroma_dir, exist_ok=True)

  # Default schema for metadata extractor
  self.schema = os.getenv("DB_SCHEMA", "sisplan")

settings = Settings()
