import asyncio
import logging
from decimal import Decimal, ROUND_DOWN
from datetime import datetime, UTC
from sqlalchemy import create_engine
import pandas as pd
import os

from binance import AsyncClient
from dotenv import load_dotenv

# ================== CONFIG ==================
SYMBOLS = ["BTCUSDT", "ETHBTC", "ETHUSDT"]
CAPITAL_USDT = Decimal("1000")
MIN_PROFIT = Decimal("0.0002")  # 0.02%

PARQUET_FILE = "trades.parquet"

# ================== LOG ==================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger()

# ================== UTILS ==================
def ajustar_quantidade(qty: Decimal, step: Decimal) -> str:
    """
    Ajusta a quantidade ao LOT_SIZE e retorna string válida para a Binance
    """
    q = (qty // step) * step
    return format(q.normalize(), 'f')

def quantize(qty, step):
    return (qty // step) * step

async def get_filters(client):
    info = await client.get_exchange_info()
    filters = {}

    for s in info["symbols"]:
        if s["symbol"] in SYMBOLS:
            f = {}
            for fl in s["filters"]:
                f[fl["filterType"]] = fl
            filters[s["symbol"]] = f

    return filters

async def get_balances(client):
    acc = await client.get_account()
    bal = {b["asset"]: Decimal(b["free"]) for b in acc["balances"]}
    for k, v in bal.items():
        if v > 0:
            log.info(f"💰 SALDO {k}: {v}")
            
    return bal

async def get_saldo_usdt(client) -> Decimal:
    acc = await client.get_account()
    
    for b in acc["balances"]:
        if b["asset"] == "USDT":
            return Decimal(b["free"])
        
    return Decimal("0")

def save_data_pg(df):
    engine = create_engine(
        "postgresql+psycopg2://neondb_owner:npg_ndgz9c2fTRHv@ep-hidden-night-ahsdposa-pooler.c-3.us-east-1.aws.neon.tech/abitrage_bot?sslmode=require&channel_binding=require"
    )
    
    df.to_sql(
        "trades",
        engine,
        if_exists="append",
        index=False
    )   
    

# ================== ARBITRAGEM ==================
async def executar():
    load_dotenv()

    log.info("Iniciando arbitragem (TESTNET)")

    client = await AsyncClient.create(
        os.getenv("BINANCE_KEY"),
        os.getenv("BINANCE_SECRET"),
        testnet=True
    )
    
    saldo_usdt_antes = await get_saldo_usdt(client)
    log.info(f"💰 USDT ANTES: {saldo_usdt_antes}")

    try:
        filtros = await get_filters(client)
        balances = await get_balances(client)

        if balances.get("USDT", 0) < CAPITAL_USDT:
            log.warning("Saldo insuficiente")
            return

        # ===== PREÇOS =====
        tickers = await client.get_orderbook_ticker()
        prices = {t["symbol"]: Decimal(t["bidPrice"]) for t in tickers if t["symbol"] in SYMBOLS}

        btc_usdt = prices["BTCUSDT"]
        eth_btc = prices["ETHBTC"]
        eth_usdt = prices["ETHUSDT"]

        # ===== SIMULAÇÃO =====
        btc = CAPITAL_USDT / btc_usdt
        eth = btc / eth_btc
        final_usdt = eth * eth_usdt

        lucro_pct = (final_usdt - CAPITAL_USDT) / CAPITAL_USDT

        log.info(f"Spread calculado: {lucro_pct:.5%}")

        if lucro_pct < MIN_PROFIT:
            log.info("Spread insuficiente")
            return

        log.info("🚀 Arbitragem válida — executando")

        # ===== FILTROS =====
        lot_btc = Decimal(filtros["BTCUSDT"]["LOT_SIZE"]["stepSize"])
        btc = quantize(btc, lot_btc)

        # ===== ORDEM 1 =====
        o1 = await client.create_order(
            symbol="BTCUSDT",
            side="BUY",
            type="MARKET",
            quoteOrderQty=str(CAPITAL_USDT)
        )

        log.info("✔ BTC comprado")

        # ===== ORDEM 2 =====
        step_eth = Decimal(filtros["ETHBTC"]["LOT_SIZE"]["stepSize"])
        qty_eth = ajustar_quantidade(eth, step_eth)

        o2 = await client.create_order(
            symbol="ETHBTC",
            side="BUY",
            type="MARKET",
            quantity=qty_eth
        )

        log.info("✔ ETH comprado")

        # ===== ORDEM 3 =====
        step_eth_usdt = Decimal(filtros["ETHUSDT"]["LOT_SIZE"]["stepSize"])
        qty_venda = ajustar_quantidade(eth, step_eth_usdt)
        
        o3 = await client.create_order(
            symbol="ETHUSDT",
            side="SELL",
            type="MARKET",
            quantity=qty_venda
        )

        log.info("✔ ETH vendido")
        
        saldo_usdt_depois = await get_saldo_usdt(client)
        log.info(f"💰 USDT DEPOIS: {saldo_usdt_depois}")


        # ===== MÉTRICAS =====
        trade = {
            "timestamp": datetime.now(UTC),
            "usdt_antes": float(saldo_usdt_antes),
            "usdt_depois": float(saldo_usdt_depois),
            "lucro_real": float(saldo_usdt_depois - saldo_usdt_antes),
            "capital_usado": float(CAPITAL_USDT),
            "status": "SUCESSO"
        }

        df = pd.DataFrame([trade])

        if os.path.exists(PARQUET_FILE):
            df_old = pd.read_parquet(PARQUET_FILE)
            df = pd.concat([df_old, df])

        df.to_parquet(PARQUET_FILE, index=False)

        log.info("📊 Trade salvo em parquet")

        save_data_pg(df)
        
        log.info("📊 Trade salvo no PostgreSQL")

        log.info("✅ Arbitragem finalizada com sucesso")
    except Exception:
        log.exception("❌ Falha na arbitragem")

    finally:
        await client.close_connection()

# ================== MAIN ==================
async def main():
    while True:
        try:
            await executar()
        except Exception:
            log.exception("Erro inesperado na execução")
        
        await asyncio.sleep(5)  # Espera 5 segundos entre execuções

if __name__ == "__main__":
    asyncio.run(main())