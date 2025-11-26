from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.routes import router
from app.db.connection import init_connection_pool, pool

app = FastAPI(
    title="Inteligência Personalizada",
    version="1.0.0",
    description="Sistema de interpretação de linguagem natural com geração SQL + PostgreSQL"
)

# --- Inicializar Pool no Startup ---
@app.on_event("startup")
def startup_event():
    print("🚀 Iniciando API...")
    init_connection_pool()
    print("🔌 Pool de conexões pronto.")


# --- Encerrar Pool no Shutdown ---
@app.on_event("shutdown")
def shutdown_event():
    if pool:
        print("🔻 Encerrando pool de conexões...")
        pool.closeall()
        print("❌ Pool encerrado com sucesso.")


# --- CORS (caso você use frontend externo) ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # ajuste conforme segurança
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Registrar Rotas ---
app.include_router(router)