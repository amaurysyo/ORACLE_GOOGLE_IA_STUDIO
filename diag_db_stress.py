# ==========================================
# file: diag_db_stress.py (CORREGIDO V3)
# ==========================================
import asyncio
import os
import time
import asyncpg
from dotenv import load_dotenv

# Cargar variables de entorno (.env)
load_dotenv()
DSN = os.getenv("PG_DSN")

# Configuración de la prueba
FILAS_TOTALES = 2000  

# DATOS: trade_id ahora es entero (i) para coincidir con BIGINT en DB
DATA_DUMMY = [
    (
        "BINANCE:PERP:BTCUSDT",  # $1: instrument_id
        i,                       # $2: trade_id_ext (INT, no str)
        90000.5,                 # $3: price
        0.001,                   # $4: qty
        "buy",                   # $5: side
        "{}"                     # $6: meta
    )
    for i in range(2000000, 2000000 + FILAS_TOTALES) # IDs muy altos
]

# SQL: Índices consecutivos $1..$6 y cast explícito para side
SQL_INSERT = """
INSERT INTO binance_futures.trades
(instrument_id, trade_id_ext, price, qty, side, meta, event_time)
VALUES ($1, $2, $3, $4, $5::side_t, $6::jsonb, NOW())
ON CONFLICT DO NOTHING
"""

async def check_zombies(conn):
    print("\n[1] Buscando bloqueos o procesos pesados en la BD...")
    rows = await conn.fetch("""
        SELECT pid, state, query_start, query 
        FROM pg_stat_activity 
        WHERE (state = 'active' OR state = 'idle in transaction')
        AND pid <> pg_backend_pid()
        AND query NOT LIKE '%pg_stat_activity%';
    """)
    if not rows:
        print("    ✅ La base de datos está limpia. No hay bloqueos.")
    else:
        print(f"    ⚠️ ATENCIÓN: Hay {len(rows)} procesos activos.")
        for r in rows[:3]:
            # Limpiamos saltos de linea para verlo mejor
            q = str(r['query']).replace('\n', ' ')[:80]
            print(f"       - PID {r['pid']} ({r['state']}): {q}...")

async def test_metodo_ametralladora(pool):
    print(f"\n[2] PRUEBA A: Insertar {FILAS_TOTALES} filas en lotes pequeños de 100 (Config Actual)...")
    print("    Simulando: batch_max_rows=100")
    
    batch_size = 100
    start = time.perf_counter()
    
    async with pool.acquire() as conn:
        chunks = [DATA_DUMMY[i:i + batch_size] for i in range(0, len(DATA_DUMMY), batch_size)]
        for i, chunk in enumerate(chunks):
            await conn.executemany(SQL_INSERT, chunk)
            
    elapsed = time.perf_counter() - start
    print(f"    ⏱️ TIEMPO TOTAL: {elapsed:.4f} segundos")
    print(f"    📊 Velocidad: {FILAS_TOTALES / elapsed:.0f} filas/seg")
    return elapsed

async def test_metodo_camion(pool):
    print(f"\n[3] PRUEBA B: Insertar {FILAS_TOTALES} filas en 1 solo lote grande (Propuesta)...")
    print("    Simulando: batch_max_rows=2000")
    
    start = time.perf_counter()
    
    async with pool.acquire() as conn:
        await conn.executemany(SQL_INSERT, DATA_DUMMY)
            
    elapsed = time.perf_counter() - start
    print(f"    ⏱️ TIEMPO TOTAL: {elapsed:.4f} segundos")
    print(f"    📊 Velocidad: {FILAS_TOTALES / elapsed:.0f} filas/seg")
    return elapsed

async def main():
    if not DSN:
        print("❌ Error: No se encontró PG_DSN en el archivo .env")
        return

    print(f"Conectando a DB... {DSN.split('@')[-1]}")
    try:
        pool = await asyncpg.create_pool(DSN)
    except Exception as e:
        print(f"❌ Error conectando: {e}")
        return

    async with pool.acquire() as conn:
        await check_zombies(conn)

    # Ejecutar pruebas
    try:
        time_a = await test_metodo_ametralladora(pool)
        time_b = await test_metodo_camion(pool)

        print("\n" + "="*40)
        print("RESULTADO DEL DIAGNÓSTICO:")
        print("="*40)
        
        if time_b > 0:
            diff = time_a / time_b
            if time_a > time_b:
                print(f"🚀 La configuración propuesta (2000) es {diff:.1f}x MÁS RÁPIDA.")
                print("   VEREDICTO: El tamaño del batch ES el problema.")
            else:
                print(f"🤔 La configuración actual (100) es {time_b/time_a:.1f}x más rápida.")
    except Exception as e:
        print(f"❌ Error durante las pruebas: {e}")
    finally:
        await pool.close()

if __name__ == "__main__":
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    try:
        asyncio.run(main())
    except RuntimeError:
        print("⚠️ Entorno interactivo detectado (Spyder). Usando nest_asyncio...")
        import nest_asyncio
        nest_asyncio.apply()
        asyncio.run(main())