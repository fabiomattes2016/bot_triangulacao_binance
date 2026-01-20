import asyncio
import json
import logging
from decimal import Decimal, ROUND_DOWN
from datetime import datetime, UTC
import time
from sqlalchemy import create_engine
import pandas as pd
import os

from binance import AsyncClient, Client
from dotenv import load_dotenv

# ================== CONFIG ==================
SYMBOLS = ["BTCUSDT", "ETHBTC", "ETHUSDT"]
CAPITAL_USDT = Decimal("200")
MIN_PROFIT = Decimal("0.0004")  # 0.02%

PARQUET_FILE = "trades.parquet"

TESTNET = True
FAKE_BALANCE = False

DUST = {
    "BTC": Decimal("0.000001"),
    "ETH": Decimal("0.00001"),
    "USDT": Decimal("0.01")
}

TRIANGLES = [
    {"base": "USDT", "inter": "BTC", "asset": "ETH"},
]

# ================== LOG ==================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger()

# ================== UTILS ==================
def symbols_from_triangle(t):
    return [
        f"{t['inter']}{t['base']}",   # BTCUSDT
        f"{t['asset']}{t['inter']}", # ETHBTC
        f"{t['asset']}{t['base']}",  # ETHUSDT
    ]

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
        if_exists="replace",
        index=False
    )   

def normalizar_saldo(valor: Decimal, asset: str) -> Decimal:
    if valor < DUST[asset]:
        return Decimal("0")
    return valor

async def verificar_saldo(moeda, fake=False):
    if fake:
        if moeda == 'USDT':
            with open("saldo_usdt.json", "r", encoding="utf-8") as arquivo:
                dados = json.load(arquivo)

                return Decimal(dados['free']) if dados else Decimal("0.0")
        elif moeda == 'BTC':
            with open("saldo_btc.json", "r", encoding="utf-8") as arquivo:
                dados = json.load(arquivo)

                return Decimal(dados['free']) if dados else Decimal("0.0")
        elif moeda == 'ETH':
            with open("saldo_eth.json", "r", encoding="utf-8") as arquivo:
                dados = json.load(arquivo)

                return Decimal(dados['free']) if dados else Decimal("0.0")
        else:
            return 0.0
    else:
        client = await AsyncClient.create(
            os.getenv("BINANCE_KEY"),
            os.getenv("BINANCE_SECRET"),
            testnet=TESTNET,
            adjust_timestamp=True
        )
        
        client.RECV_WINDOW = 60000
        
        server_time = await client.get_server_time()
        server_timestamp = server_time["serverTime"]

        client.timestamp_offset = server_timestamp - int(datetime.now(UTC).timestamp() * 1000)
        
        saldo = client.get_asset_balance(asset=moeda)

        return Decimal(saldo['free']) if saldo else Decimal("0.0")

def atualizar_saldo_fake(moeda, novo_saldo):
    if moeda == 'USDT':
        with open("saldo_usdt.json", "r", encoding="utf-8") as f:
            dados = json.load(f)

        # Atualizar valores
        dados["free"] = str(novo_saldo)

        # Salvar novamente no arquivo
        with open("saldo_usdt.json", "w", encoding="utf-8") as f:
            json.dump(dados, f, indent=4)
    elif moeda == 'BTC':
        with open("saldo_btc.json", "r", encoding="utf-8") as f:
            dados = json.load(f)

        # Atualizar valores
        dados["free"] = str(novo_saldo)

        # Salvar novamente no arquivo
        with open("saldo_btc.json", "w", encoding="utf-8") as f:
            json.dump(dados, f, indent=4)
    elif moeda == 'ETH':
        with open("saldo_eth.json", "r", encoding="utf-8") as f:
            dados = json.load(f)

        # Atualizar valores
        dados["free"] = str(novo_saldo)

        # Salvar novamente no arquivo
        with open("saldo_eth.json", "w", encoding="utf-8") as f:
            json.dump(dados, f, indent=4)

# ================== ARBITRAGEM ==================
async def executar():
    load_dotenv()

    log.info("Iniciando arbitragem (TESTNET)")
    
    for t in TRIANGLES:
        base = t["base"]
        inter = t["inter"]
        asset = t["asset"]

        sym1 = f"{inter}{base}"
        sym2 = f"{asset}{inter}"
        sym3 = f"{asset}{base}"

        log.info(f"🔺 Testando triângulo {base} → {inter} → {asset} → {base}")

    client = await AsyncClient.create(
        os.getenv("BINANCE_KEY"),
        os.getenv("BINANCE_SECRET"),
        testnet=TESTNET,
    )
    
    client.RECV_WINDOW = 60000
    
    server_time = await client.get_server_time()
    server_timestamp = server_time["serverTime"]

    client.timestamp_offset = server_timestamp - int(datetime.now(UTC).timestamp() * 1000)
    
    saldo_usdt_antes = Decimal("0")
    saldo_usdt_depois = Decimal("0")
    
    if FAKE_BALANCE:
        saldo_usdt_antes = await verificar_saldo('USDT', fake=FAKE_BALANCE)
        log.info(f"💰 SALDO USDT (FAKE): {saldo_usdt_antes}")
    else:
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
        p1 = prices[sym1]  # BTCUSDT
        p2 = prices[sym2]  # ETHBTC
        p3 = prices[sym3]  # ETHUSDT

        q_inter = CAPITAL_USDT / p1
        q_asset = q_inter / p2
        final_base = q_asset * p3

        lucro_pct = (final_base - CAPITAL_USDT) / CAPITAL_USDT

        log.info(f"Spread calculado: {lucro_pct:.5%}")

        if lucro_pct < MIN_PROFIT:
            log.info("Spread insuficiente")
            return

        log.info("🚀 Arbitragem válida — executando")

        # ===== FILTROS =====
        for s in (sym1, sym2, sym3):
            if s not in filtros:
                log.warning(f"{s} não disponível")
                continue
        
        lot_btc = Decimal(filtros["BTCUSDT"]["LOT_SIZE"]["stepSize"])
        btc = quantize(btc, lot_btc)

        # ===== ORDEM 1 =====
        if saldo_usdt_antes < CAPITAL_USDT:
            log.warning("Saldo USDT insuficiente")
            return
        
        o1 = await client.create_order(
            symbol=sym1,
            side="BUY",
            type="MARKET",
            quoteOrderQty=str(CAPITAL_USDT)
        )

        inter_recebido = Decimal(o1["executedQty"])

        log.info("✔ BTC comprado")

        # ===== ORDEM 2 =====
        saldo_inter = await verificar_saldo(inter, fake=FAKE_BALANCE)

        step_asset = Decimal(filtros[sym2]["LOT_SIZE"]["stepSize"])

        inter_para_usar = saldo_inter * Decimal("0.999")
        qty_asset = quantize(inter_para_usar / p2, step_asset)

        o2 = await client.create_order(
            symbol=sym2,
            side="BUY",
            type="MARKET",
            quantity=str(qty_asset)
        )

        asset_recebido = Decimal(o2["executedQty"])

        log.info("✔ ETH comprado")

        # ===== ORDEM 3 =====
        saldo_asset = await verificar_saldo(asset, fake=FAKE_BALANCE)

        step_asset_base = Decimal(filtros[sym3]["LOT_SIZE"]["stepSize"])

        asset_para_vender = saldo_asset * Decimal("0.999")
        qty_venda = quantize(asset_para_vender, step_asset_base)

        o3 = await client.create_order(
            symbol=sym3,
            side="SELL",
            type="MARKET",
            quantity=str(qty_venda)
        )

        log.info("✔ ETH vendido")
        
        if FAKE_BALANCE:
            saldo_usdt_depois = await verificar_saldo('USDT', fake=FAKE_BALANCE)
            log.info(f"💰 SALDO USDT DEPOIS (FAKE): {saldo_usdt_depois}")
        else:
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