"""
stock_report.py  —  v4
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Paniere ~3.200 titoli scaricato dinamicamente da Wikipedia e fonti
pubbliche — copre indici principali E secondari di ogni mercato.

Email mar–sab : top 20 perdite + top 20 guadagni del giorno
Email lunedì  : top 20 perdite + top 20 guadagni della settimana

Mercati: USA · Londra · Milano · Parigi · Francoforte · Amsterdam
         Madrid · Zurigo · Tokyo · Seoul · Hong Kong · Sydney
         San Paolo · Mumbai

Credenziali via GitHub Secrets:
  EMAIL_MITTENTE · APP_PASSWORD · EMAIL_DESTINATARIO
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import os, ssl, smtplib, warnings, time, re
from datetime import datetime, timedelta, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import pandas as pd
import requests
import yfinance as yf

warnings.filterwarnings("ignore")

# ── Credenziali ────────────────────────────────────────────────────────────────
EMAIL_MITTENTE     = os.environ["EMAIL_MITTENTE"]
APP_PASSWORD       = os.environ["APP_PASSWORD"]
EMAIL_DESTINATARIO = os.environ["EMAIL_DESTINATARIO"]

# ── Funzioni fetch dinamiche da Wikipedia ──────────────────────────────────────

def _wiki(url, col_ticker, suffix="", col_alt=None, max_retries=2):
    """Scarica tabella Wikipedia ed estrae ticker Yahoo Finance."""
    headers = {"User-Agent": "Mozilla/5.0 (compatible; stockreport/4.0)"}
    for attempt in range(max_retries):
        try:
            tables = pd.read_html(url, attrs=None, header=0, flavor="lxml")
            for t in tables:
                cols = [str(c).strip() for c in t.columns]
                target = next((c for c in cols if col_ticker.lower() in c.lower()), None)
                if target is None and col_alt:
                    target = next((c for c in cols if col_alt.lower() in c.lower()), None)
                if target:
                    raw = t[target].dropna().astype(str).tolist()
                    out = []
                    for r in raw:
                        r = r.strip().split("\n")[0].split(" ")[0]
                        r = re.sub(r"[^A-Z0-9.\-]", "", r.upper())
                        if 1 < len(r) <= 12:
                            tk = r + suffix if suffix and not r.endswith(suffix) else r
                            out.append(tk)
                    if out:
                        return out
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(2)
            else:
                print(f"  ⚠ fetch fallito ({url[:60]}…): {e}")
    return []

# ── USA ────────────────────────────────────────────────────────────────────────

def fetch_sp500():
    t = _wiki("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies", "Symbol")
    return [x.replace(".", "-") for x in t]

def fetch_sp400():
    t = _wiki("https://en.wikipedia.org/wiki/List_of_S%26P_400_companies", "Ticker")
    return [x.replace(".", "-") for x in t]

def fetch_sp600():
    t = _wiki("https://en.wikipedia.org/wiki/List_of_S%26P_600_companies", "Ticker")
    return [x.replace(".", "-") for x in t]

def fetch_nasdaq100():
    return _wiki("https://en.wikipedia.org/wiki/Nasdaq-100", "Ticker")

# ── UK ─────────────────────────────────────────────────────────────────────────

def fetch_ftse100():
    return _wiki("https://en.wikipedia.org/wiki/FTSE_100_Index", "Ticker", suffix=".L")

def fetch_ftse250():
    return _wiki("https://en.wikipedia.org/wiki/FTSE_250_Index", "Ticker", suffix=".L")

# ── Italia ─────────────────────────────────────────────────────────────────────

def fetch_ftse_mib():
    # Wikipedia non ha tabella stabile — lista curata manualmente
    return [
        "A2A.MI","AMP.MI","ATL.MI","BAMI.MI","BMED.MI","BPSO.MI","BZU.MI",
        "CPR.MI","DIA.MI","ENEL.MI","ENI.MI","ERG.MI","FBK.MI","G.MI",
        "GEO.MI","HER.MI","INW.MI","ISP.MI","LDO.MI","MB.MI","MONC.MI",
        "NEXI.MI","PIRC.MI","PRY.MI","RACE.MI","REC.MI","SRG.MI","STM.MI",
        "TEN.MI","TERNA.MI","TIT.MI","TRN.MI","UCG.MI","UNI.MI","IP.MI",
        "PST.MI","AZM.MI","WEBUILD.MI","ITW.MI","ACSM.MI",
    ]

def fetch_ftse_italia_mid():
    return [
        "CVAL.MI","IGD.MI","IVS.MI","MARR.MI","OVS.MI","SECO.MI",
        "TFI.MI","TOD.MI","VLNV.MI","ZUC.MI","AEFFE.MI","AMBR.MI",
        "ASTM.MI","BC.MI","CALZ.MI","CASS.MI","CEME.MI","CFT.MI",
        "CLT.MI","CMS.MI","COVA.MI","CVS.MI","DBP.MI","DGVE.MI",
        "DLG.MI","DMAIL.MI","EI.MI","EMAK.MI","ENAV.MI","ENR.MI",
    ]

# ── Francia ────────────────────────────────────────────────────────────────────

def fetch_cac40():
    return _wiki("https://en.wikipedia.org/wiki/CAC_40", "Ticker", suffix=".PA")

def fetch_sbf120():
    t = _wiki("https://en.wikipedia.org/wiki/SBF_120", "Ticker", suffix=".PA")
    return t if t else [
        "AF.PA","AGN.PA","AIR.PA","AKKA.PA","ALD.PA","ALO.PA","ALT.PA",
        "AMUN.PA","ATO.PA","AXA.PA","BB.PA","BEN.PA","BIM.PA","BLC.PA",
        "BNPP.PA","BOL.PA","BVI.PA","CAP.PA","CAPR.PA","CBE.PA","CHSR.PA",
        "CNP.PA","CO.PA","COFA.PA","COX.PA","CRI.PA","DBV.PA","DSY.PA",
        "DUR.PA","EDEN.PA","EDF.PA","EI.PA","ELIOR.PA","ENGI.PA","ENX.PA",
        "ERF.PA","ESSO.PA","ETL.PA","EUC.PA","EUCAR.PA","FDR.PA","FGR.PA",
        "FP.PA","FTE.PA","GAM.PA","GET.PA","GPRO.PA","GTT.PA","HO.PA",
        "IDS.PA","IMERYS.PA","INF.PA","IPSOS.PA","JXR.PA","KER.PA","KLIMA.PA",
        "LAC.PA","LHN.PA","LI.PA","LNA.PA","LNR.PA","LR.PA","LSS.PA",
        "LTO.PA","MANUF.PA","MC.PA","MERY.PA","MF.PA","ML.PA","MMB.PA",
        "MRN.PA","MTU.PA","NEX.PA","NXI.PA","ORA.PA","OR.PA","OSE.PA",
        "POM.PA","PUB.PA","RCO.PA","RNO.PA","RUI.PA","SAF.PA","SGO.PA",
    ]

# ── Germania ───────────────────────────────────────────────────────────────────

def fetch_dax():
    return _wiki("https://en.wikipedia.org/wiki/DAX", "Ticker", suffix=".DE")

def fetch_mdax():
    return _wiki("https://en.wikipedia.org/wiki/MDAX", "Ticker", suffix=".DE")

def fetch_sdax():
    return _wiki("https://en.wikipedia.org/wiki/SDAX", "Ticker", suffix=".DE")

# ── Olanda ─────────────────────────────────────────────────────────────────────

def fetch_aex():
    return _wiki("https://en.wikipedia.org/wiki/AEX_index", "Ticker", suffix=".AS")

def fetch_amx():
    return _wiki("https://en.wikipedia.org/wiki/AMX_index", "Ticker", suffix=".AS")

# ── Spagna ─────────────────────────────────────────────────────────────────────

def fetch_ibex35():
    return _wiki("https://en.wikipedia.org/wiki/IBEX_35", "Ticker", suffix=".MC")

# ── Svizzera ───────────────────────────────────────────────────────────────────

def fetch_smi():
    return _wiki("https://en.wikipedia.org/wiki/Swiss_Market_Index", "Ticker", suffix=".SW")

def fetch_spi():
    t = _wiki("https://en.wikipedia.org/wiki/Swiss_Performance_Index", "Ticker", suffix=".SW")
    return t if t else [
        "ABBN.SW","ADEN.SW","ALC.SW","ALLN.SW","ALSN.SW","BAER.SW","BARN.SW",
        "BCGE.SW","BCVN.SW","BKW.SW","BLKB.SW","BUCN.SW","BVZN.SW","CALN.SW",
        "CFT.SW","CICN.SW","CLTN.SW","CMBN.SW","COOP.SW","COTN.SW","CPGN.SW",
        "DESN.SW","DKSH.SW","DUFN.SW","EFGN.SW","EMMN.SW","EMSN.SW","EPEN.SW",
        "ESTC.SW","FHZN.SW","FORN.SW","GAIN.SW","GEBN.SW","GIVN.SW","HELN.SW",
        "HKTM.SW","HOLN.SW","HUBN.SW","INRN.SW","IREN.SW","JBCG.SW","JFIN.SW",
        "KNIN.SW","KOMN.SW","KURN.SW","LEHN.SW","LISN.SW","LKQS.SW","LOHN.SW",
        "LSN.SW","LUKN.SW","MCHN.SW","METN.SW","MILN.SW","MOBN.SW","MSGN.SW",
        "MYRN.SW","NESN.SW","NOVN.SW","NREN.SW","ORON.SW","PATN.SW","PGHN.SW",
        "PSPN.SW","RBBN.SW","ROG.SW","SALN.SW","SCMN.SW","SCOL.SW","SFSN.SW",
        "SGKN.SW","SGSN.SW","SHN.SW","SISE.SW","SLHN.SW","SLOG.SW","SNBN.SW",
        "SOFN.SW","SOON.SW","SRAIL.SW","STGN.SW","STMN.SW","STRN.SW","SUNN.SW",
        "TEMN.SW","TIBN.SW","TISG.SW","TPHN.SW","UBSG.SW","UHRN.SW","VACN.SW",
        "VET.SW","VIFN.SW","VILLN.SW","VLN.SW","VONN.SW","WARN.SW","WIFN.SW",
        "ZULN.SW","ZURN.SW",
    ]

# ── Giappone ───────────────────────────────────────────────────────────────────

def fetch_nikkei225():
    return _wiki("https://en.wikipedia.org/wiki/Nikkei_225", "Code", suffix=".T")

def fetch_topix100():
    # Titoli TOPIX aggiuntivi oltre il Nikkei
    return [
        "1301.T","1332.T","1333.T","1605.T","1721.T","1801.T","1802.T",
        "1803.T","1808.T","1812.T","1821.T","1925.T","1928.T","2002.T",
        "2269.T","2282.T","2413.T","2502.T","2503.T","2531.T","2768.T",
        "2801.T","2802.T","2871.T","2914.T","3086.T","3099.T","3101.T",
        "3103.T","3105.T","3231.T","3289.T","3382.T","3401.T","3402.T",
        "3407.T","3436.T","3526.T","3563.T","3659.T","3861.T","3865.T",
        "3893.T","4004.T","4005.T","4021.T","4042.T","4043.T","4061.T",
        "4151.T","4183.T","4188.T","4208.T","4272.T","4307.T","4324.T",
        "4385.T","4452.T","4523.T","4528.T","4543.T","4568.T","4578.T",
        "4612.T","4689.T","4704.T","4751.T","4755.T","4901.T","4902.T",
        "5020.T","5101.T","5108.T","5201.T","5202.T","5214.T","5301.T",
        "5332.T","5333.T","5401.T","5406.T","5411.T","5413.T","5440.T",
        "5463.T","5541.T","5631.T","5703.T","5706.T","5707.T","5711.T",
        "5713.T","5714.T","5801.T","5802.T","5803.T","5901.T","5991.T",
    ]

# ── Corea del Sud ──────────────────────────────────────────────────────────────

def fetch_kospi():
    return [
        "005930.KS","000660.KS","035420.KS","005380.KS","051910.KS",
        "006400.KS","035720.KS","000270.KS","068270.KS","105560.KS",
        "028260.KS","012330.KS","018880.KS","066570.KS","003550.KS",
        "032830.KS","096770.KS","034020.KS","011200.KS","042660.KS",
        "010130.KS","009150.KS","000810.KS","011170.KS","003490.KS",
        "055550.KS","316140.KS","086790.KS","138930.KS","024110.KS",
        "000100.KS","009540.KS","010950.KS","033780.KS","271560.KS",
        "017670.KS","030200.KS","036570.KS","251270.KS","034730.KS",
        "018260.KS","008770.KS","078930.KS","047050.KS","011070.KS",
        "259960.KS","207940.KS","352820.KS","041510.KS","003670.KS",
        "006800.KS","010140.KS","011790.KS","012450.KS","015760.KS",
        "016360.KS","017800.KS","020150.KS","021240.KS","023530.KS",
        "024520.KS","025840.KS","026960.KS","029780.KS","030000.KS",
        "032640.KS","033240.KS","034950.KS","035250.KS","036460.KS",
        "037270.KS","039130.KS","040910.KS","042700.KS","044490.KS",
        "047810.KS","051600.KS","053000.KS","055490.KS","057050.KS",
        "058850.KS","060980.KS","063160.KS","064350.KS","069960.KS",
        "071050.KS","075580.KS","079550.KS","082640.KS","083420.KS",
        "084370.KS","085620.KS","086280.KS","088350.KS","090350.KS",
        "093370.KS","095570.KS","097950.KS","100840.KS","102110.KS",
    ]

# ── Hong Kong ──────────────────────────────────────────────────────────────────

def fetch_hangseng():
    return _wiki("https://en.wikipedia.org/wiki/Hang_Seng_Index", "Code", suffix=".HK")

def fetch_hk_tech():
    return _wiki("https://en.wikipedia.org/wiki/Hang_Seng_Tech_Index", "Ticker", suffix=".HK")

def fetch_hk_extra():
    return [
        "1.HK","2.HK","3.HK","4.HK","5.HK","6.HK","8.HK","11.HK",
        "12.HK","16.HK","17.HK","19.HK","20.HK","23.HK","27.HK","66.HK",
        "83.HK","101.HK","119.HK","135.HK","144.HK","151.HK","168.HK",
        "175.HK","179.HK","189.HK","200.HK","213.HK","215.HK","220.HK",
        "267.HK","268.HK","270.HK","285.HK","291.HK","293.HK","297.HK",
        "302.HK","303.HK","322.HK","328.HK","330.HK","336.HK","338.HK",
        "341.HK","345.HK","347.HK","354.HK","358.HK","362.HK","371.HK",
        "386.HK","388.HK","390.HK","392.HK","396.HK","400.HK","403.HK",
        "410.HK","425.HK","431.HK","440.HK","489.HK","494.HK","522.HK",
        "525.HK","531.HK","535.HK","543.HK","548.HK","551.HK","552.HK",
        "568.HK","576.HK","579.HK","586.HK","588.HK","590.HK","598.HK",
        "600.HK","606.HK","607.HK","608.HK","609.HK","611.HK","636.HK",
        "639.HK","656.HK","658.HK","669.HK","673.HK","688.HK","694.HK",
        "696.HK","700.HK","709.HK","719.HK","728.HK","737.HK","746.HK",
        "762.HK","772.HK","788.HK","799.HK","806.HK","810.HK","811.HK",
        "813.HK","819.HK","823.HK","825.HK","836.HK","837.HK","839.HK",
        "857.HK","867.HK","868.HK","869.HK","874.HK","880.HK","882.HK",
        "883.HK","884.HK","885.HK","887.HK","888.HK","902.HK","909.HK",
        "914.HK","916.HK","921.HK","939.HK","941.HK","945.HK","951.HK",
        "960.HK","966.HK","968.HK","981.HK","992.HK","998.HK","1024.HK",
        "1038.HK","1044.HK","1055.HK","1066.HK","1071.HK","1072.HK",
        "1093.HK","1099.HK","1109.HK","1113.HK","1128.HK","1138.HK",
        "1177.HK","1186.HK","1193.HK","1199.HK","1209.HK","1211.HK",
        "1288.HK","1299.HK","1302.HK","1336.HK","1339.HK","1347.HK",
        "1357.HK","1378.HK","1382.HK","1398.HK","1548.HK","1579.HK",
        "1810.HK","1818.HK","1876.HK","1928.HK","1929.HK","1972.HK",
        "2007.HK","2018.HK","2020.HK","2038.HK","2188.HK","2196.HK",
        "2238.HK","2269.HK","2282.HK","2313.HK","2318.HK","2319.HK",
        "2328.HK","2331.HK","2333.HK","2338.HK","2382.HK","2388.HK",
        "2600.HK","2601.HK","2628.HK","2638.HK","2688.HK","2689.HK",
        "2727.HK","2799.HK","2800.HK","2899.HK","3328.HK","3333.HK",
        "3618.HK","3628.HK","3633.HK","3690.HK","3692.HK","3698.HK",
        "3908.HK","3968.HK","3988.HK","6030.HK","6060.HK","6098.HK",
        "6110.HK","6618.HK","6690.HK","6862.HK","6969.HK","9618.HK",
        "9633.HK","9888.HK","9961.HK","9988.HK","9999.HK",
    ]

# ── Australia ──────────────────────────────────────────────────────────────────

def fetch_asx200():
    return _wiki("https://en.wikipedia.org/wiki/S%26P/ASX_200", "Code", suffix=".AX")

# ── Brasile ────────────────────────────────────────────────────────────────────

def fetch_bovespa():
    t = _wiki("https://en.wikipedia.org/wiki/List_of_companies_listed_on_B3",
              "Ticker", suffix=".SA")
    if not t:
        t = _wiki("https://en.wikipedia.org/wiki/Ibovespa", "Ticker", suffix=".SA")
    if not t:
        t = [
            "ABEV3.SA","ALPA4.SA","AMER3.SA","ASAI3.SA","AZUL4.SA",
            "B3SA3.SA","BBAS3.SA","BBDC3.SA","BBDC4.SA","BBSE3.SA",
            "BEEF3.SA","BPAC11.SA","BRAP4.SA","BRFS3.SA","BRKM5.SA",
            "BRML3.SA","CASH3.SA","CCRO3.SA","CIEL3.SA","CMIG4.SA",
            "CMIN3.SA","COGN3.SA","CPFE3.SA","CPLE6.SA","CRFB3.SA",
            "CSAN3.SA","CSNA3.SA","CVCB3.SA","CYRE3.SA","DXCO3.SA",
            "ECOR3.SA","EGIE3.SA","ELET3.SA","ELET6.SA","EMBR3.SA",
            "ENBR3.SA","ENEV3.SA","ENGI11.SA","EQTL3.SA","EZTC3.SA",
            "FLRY3.SA","GGBR4.SA","GOAU4.SA","GOLL4.SA","HAPV3.SA",
            "HYPE3.SA","IGTI11.SA","IRBR3.SA","ITSA4.SA","ITUB4.SA",
            "JBSS3.SA","JHSF3.SA","KLBN11.SA","LREN3.SA","LWSA3.SA",
            "MGLU3.SA","MRFG3.SA","MRVE3.SA","MULT3.SA","NTCO3.SA",
            "PCAR3.SA","PETR3.SA","PETR4.SA","PETZ3.SA","PRIO3.SA",
            "QUAL3.SA","RADL3.SA","RAIL3.SA","RAIZ4.SA","RDOR3.SA",
            "RENT3.SA","RRRP3.SA","SANB11.SA","SBSP3.SA","SLCE3.SA",
            "SMTO3.SA","SOMA3.SA","STBP3.SA","SUZB3.SA","TAEE11.SA",
            "TIMS3.SA","TOTS3.SA","UGPA3.SA","USIM5.SA","VALE3.SA",
            "VBBR3.SA","VIIA3.SA","VIVT3.SA","WEGE3.SA","YDUQ3.SA",
        ]
    return t

# ── India ──────────────────────────────────────────────────────────────────────

def fetch_nifty50():
    return _wiki("https://en.wikipedia.org/wiki/NIFTY_50", "Symbol", suffix=".NS")

def fetch_nifty100():
    return _wiki("https://en.wikipedia.org/wiki/Nifty_100", "Symbol", suffix=".NS")

def fetch_sensex():
    return _wiki("https://en.wikipedia.org/wiki/BSE_SENSEX", "Symbol", suffix=".BO")

# ── Registro indici ────────────────────────────────────────────────────────────
# (label, borsa, ticker_yahoo_indice, fetch_fn)
INDICI_CONFIG = [
    # USA
    ("S&P 500",        "New York (NYSE)",    "^GSPC",      fetch_sp500),
    ("S&P 400",        "New York (NYSE)",    "^MID",       fetch_sp400),
    ("S&P 600",        "New York (NYSE)",    "^SML",       fetch_sp600),
    ("Nasdaq 100",     "New York (NASDAQ)",  "^NDX",       fetch_nasdaq100),
    # UK
    ("FTSE 100",       "Londra (LSE)",       "^FTSE",      fetch_ftse100),
    ("FTSE 250",       "Londra (LSE)",       "^FTMC",      fetch_ftse250),
    # Italia
    ("FTSE MIB",       "Milano (BIT)",       "FTSEMIB.MI", fetch_ftse_mib),
    ("FTSE Italia Mid","Milano (BIT)",       "FTSEMIB.MI", fetch_ftse_italia_mid),
    # Francia
    ("CAC 40",         "Parigi (EPA)",       "^FCHI",      fetch_cac40),
    ("SBF 120",        "Parigi (EPA)",       "^SBF120",    fetch_sbf120),
    # Germania
    ("DAX",            "Francoforte (XETRA)","^GDAXI",     fetch_dax),
    ("MDAX",           "Francoforte (XETRA)","^MDAXI",     fetch_mdax),
    ("SDAX",           "Francoforte (XETRA)","^SDAXI",     fetch_sdax),
    # Olanda
    ("AEX",            "Amsterdam (AEX)",    "^AEX",       fetch_aex),
    ("AMX",            "Amsterdam (AEX)",    "^AMX",       fetch_amx),
    # Spagna
    ("IBEX 35",        "Madrid (BME)",       "^IBEX",      fetch_ibex35),
    # Svizzera
    ("SMI",            "Zurigo (SIX)",       "^SSMI",      fetch_smi),
    ("SPI",            "Zurigo (SIX)",       "^SSMI",      fetch_spi),
    # Giappone
    ("Nikkei 225",     "Tokyo (TSE)",        "^N225",      fetch_nikkei225),
    ("TOPIX",          "Tokyo (TSE)",        "^TPX",       fetch_topix100),
    # Corea
    ("KOSPI",          "Seoul (KRX)",        "^KS11",      fetch_kospi),
    # Hong Kong
    ("Hang Seng",      "Hong Kong (HKEX)",   "^HSI",       fetch_hangseng),
    ("HS Tech",        "Hong Kong (HKEX)",   "^HSTECH",    fetch_hk_tech),
    ("HKEX Extra",     "Hong Kong (HKEX)",   "^HSI",       fetch_hk_extra),
    # Australia
    ("ASX 200",        "Sydney (ASX)",       "^AXJO",      fetch_asx200),
    # Brasile
    ("Bovespa",        "São Paulo (B3)",     "^BVSP",      fetch_bovespa),
    # India
    ("Nifty 50",       "Mumbai (NSE)",       "^NSEI",      fetch_nifty50),
    ("Nifty 100",      "Mumbai (NSE)",       "^NSEI",      fetch_nifty100),
    ("BSE Sensex",     "Mumbai (BSE)",       "^BSESN",     fetch_sensex),
]

# ── Costruzione paniere ────────────────────────────────────────────────────────

def build_universe():
    ticker_meta = {}
    print("🌍 Scaricamento componenti indici da Wikipedia...")
    for label, borsa, _, fetch_fn in INDICI_CONFIG:
        tks = fetch_fn()
        print(f"   {label:20s} → {len(tks):4d}  ({borsa})")
        for tk in tks:
            if tk not in ticker_meta:
                ticker_meta[tk] = {"indice": label, "borsa": borsa}
            else:
                prev = ticker_meta[tk]["indice"]
                if label not in prev:
                    ticker_meta[tk]["indice"] = prev + " / " + label
    all_tickers = list(ticker_meta.keys())
    print(f"   {'─'*45}")
    print(f"   Totale titoli unici: {len(all_tickers)}")
    return all_tickers, ticker_meta

# ── Download prezzi ────────────────────────────────────────────────────────────

def scarica_prezzi(tickers, giorni=45):
    end   = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    start = (datetime.now(timezone.utc) - timedelta(days=giorni)).strftime("%Y-%m-%d")
    chunks = [tickers[i:i+200] for i in range(0, len(tickers), 200)]
    frames = []
    print(f"⬇️  Download prezzi in {len(chunks)} batch...")
    for i, chunk in enumerate(chunks):
        print(f"   Batch {i+1:2d}/{len(chunks)} — {len(chunk)} titoli", end=" ")
        for attempt in range(3):
            try:
                data  = yf.download(chunk, start=start, end=end,
                                    progress=False, auto_adjust=True, threads=True)
                close = data["Close"] if isinstance(data.columns, pd.MultiIndex) else data
                frames.append(close)
                print(f"✓ ({close.shape[1]} ok)")
                break
            except Exception as e:
                if attempt < 2:
                    print(f"retry{attempt+1}", end=" ")
                    time.sleep(3)
                else:
                    print(f"✗ ({e})")
        time.sleep(0.8)
    if not frames:
        return pd.DataFrame()
    result = pd.concat(frames, axis=1)
    result = result.loc[:, ~result.columns.duplicated()]
    return result.dropna(how="all", axis=1)

def scarica_indici():
    # Indici unici (rimuovi duplicati per ticker)
    seen = set()
    unici = []
    for label, borsa, sym, _ in INDICI_CONFIG:
        if sym not in seen:
            seen.add(sym)
            unici.append((label, borsa, sym))

    syms  = [s for _, _, s in unici]
    end   = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    start = (datetime.now(timezone.utc) - timedelta(days=10)).strftime("%Y-%m-%d")
    try:
        data  = yf.download(syms, start=start, end=end,
                            progress=False, auto_adjust=True, threads=True)
        close = data["Close"] if isinstance(data.columns, pd.MultiIndex) else data
        variaz = close.pct_change().iloc[-1] * 100
    except Exception:
        variaz = pd.Series(dtype=float)

    result = {}
    for label, borsa, sym in unici:
        v = float(variaz.get(sym, float("nan")))
        result[label] = {"nome": label, "borsa": borsa, "var": v}
    return result

def scarica_info_batch(tickers):
    info_map = {}
    chunks = [tickers[i:i+80] for i in range(0, len(tickers), 80)]
    print(f"⬇️  Download info per {len(tickers)} titoli finalisti ({len(chunks)} batch)...")
    for i, chunk in enumerate(chunks):
        print(f"   Info batch {i+1}/{len(chunks)}", end=" ")
        try:
            batch = yf.Tickers(" ".join(chunk))
            for tk in chunk:
                try:
                    inf = batch.tickers[tk].info
                    info_map[tk] = {
                        "name":   inf.get("longName") or inf.get("shortName") or tk,
                        "mktcap": inf.get("marketCap"),
                    }
                except Exception:
                    info_map[tk] = {"name": tk, "mktcap": None}
            print("✓")
        except Exception as e:
            print(f"✗ ({e})")
            for tk in chunk:
                info_map[tk] = {"name": tk, "mktcap": None}
        time.sleep(0.5)
    return info_map

# ── Calcolo variazioni ─────────────────────────────────────────────────────────

def calc_variazioni(close):
    rows = []
    for col in close.columns:
        s = close[col].dropna()
        if len(s) < 2: continue
        p0  = float(s.iloc[-1])
        p1  = float(s.iloc[-2])
        v1d = (p0/p1 - 1)*100
        p7  = float(s.iloc[-8]  if len(s) >= 8  else s.iloc[0])
        p30 = float(s.iloc[-31] if len(s) >= 31 else s.iloc[0])
        v7d  = (p0/p7  - 1)*100
        v30d = (p0/p30 - 1)*100
        rows.append({"ticker": col, "prezzo": p0,
                     "var_1d": v1d, "var_7d": v7d, "var_30d": v30d})
    return pd.DataFrame(rows).set_index("ticker")

def calc_variazioni_settimanali(close):
    rows = []
    for col in close.columns:
        s = close[col].dropna()
        if len(s) < 2: continue
        p0 = float(s.iloc[-1])
        pi = float(s.iloc[-6] if len(s) >= 6 else s.iloc[0])
        rows.append({"ticker": col, "prezzo": p0,
                     "var_settimana": (p0/pi - 1)*100})
    return pd.DataFrame(rows).set_index("ticker")

# ── Helpers HTML ───────────────────────────────────────────────────────────────

def fmt_pct(v):
    if pd.isna(v): return "—"
    c = "#cc0000" if v < 0 else "#007700"
    a = "▼" if v < 0 else "▲"
    return f'<span style="color:{c};font-weight:bold">{a} {v:+.2f}%</span>'

def fmt_cap(v):
    if not v or pd.isna(v): return "—"
    if v >= 1e12: return f"{v/1e12:.1f}T"
    if v >= 1e9:  return f"{v/1e9:.1f}B"
    return f"{v/1e6:.0f}M"

def ylink(ticker, name):
    # Nome azienda come testo cliccabile → Yahoo Finance
    y_url = f"https://finance.yahoo.com/quote/{ticker}"
    # Google Finance come alternativa
    suffix_to_goog = {
        ".PA": "EPA", ".MI": "BIT", ".DE": "XETRA", ".L": "LON",
        ".AS": "AMS", ".MC": "BME", ".SW": "SWX", ".T": "TYO",
        ".KS": "KRX", ".HK": "HKEX", ".AX": "ASX", ".SA": "BVMF",
        ".NS": "NSE", ".BO": "BOM",
    }
    g_prefix = next((v for k, v in suffix_to_goog.items() if ticker.endswith(k)), None)
    g_base   = ticker.split(".")[0] if "." in ticker else ticker
    g_url    = (f"https://www.google.com/finance/quote/{g_base}:{g_prefix}"
                if g_prefix else f"https://www.google.com/finance/quote/{ticker}:NASDAQ")
    safe = str(name).replace('"', "&quot;")
    disp = (safe[:36] + "…") if len(safe) > 36 else safe
    return (f'<a href="{y_url}" title="Apri su Yahoo Finance" '
            f'style="color:#1a1a2e;text-decoration:none;font-weight:bold">{disp}</a>'
            f' &nbsp;<a href="{g_url}" title="Apri su Google Finance" '
            f'style="color:#888;text-decoration:none;font-size:10px">[G]</a>')

def fmt_price(v): return f"{v:.2f}" if not pd.isna(v) else "—"

def indici_bar(indici_info):
    items = ""
    for d in indici_info.values():
        v  = d["var"]
        ok = not pd.isna(v)
        c  = "#ff4444" if (ok and v < 0) else "#44dd77"
        a  = "▼" if (ok and v < 0) else "▲"
        val = f"{a} {v:+.2f}%" if ok else "—"
        items += (f'<div style="text-align:center;padding:8px 10px;'
                  f'background:#2d3a4a;border-radius:6px;min-width:85px;flex:1 1 85px;'
                  f'border:1px solid #3d4f63">'
                  f'<div style="font-size:10px;color:#c8d8e8;margin-bottom:2px;font-weight:bold">{d["nome"]}</div>'
                  f'<div style="font-size:13px;font-weight:bold;color:{c}">{val}</div>'
                  f'<div style="font-size:9px;color:#8aaabb">{d["borsa"]}</div></div>')
    return (f'<div style="background:#1a2535;padding:14px 16px;border-radius:8px;'
            f'display:flex;flex-wrap:wrap;gap:8px;margin-bottom:22px">{items}</div>')

# ── Costruzione tabelle ────────────────────────────────────────────────────────

TD   = "padding:7px 6px;font-size:12px;border-bottom:1px solid #f0f0f0"
TH_S = "padding:8px 6px;font-size:11px;white-space:nowrap;text-align:{a}"

def _thead(cols):
    ths = "".join(f'<th style="{TH_S.format(a=a)}">{n}</th>' for n, a in cols)
    return f'<thead><tr style="background:#1a1a2e;color:white">{ths}</tr></thead>'

THEAD_DAILY = _thead([
    ("#","center"),("Titolo","left"),("Borsa","left"),("Indice","left"),
    ("Prezzo","right"),("Ieri","right"),("7gg","right"),("30gg","right"),("Cap.","right")
])
THEAD_WEEKLY = _thead([
    ("#","center"),("Titolo","left"),("Borsa","left"),("Indice","left"),
    ("Prezzo","right"),("Settimana","right"),("Cap.","right")
])

def _rows_daily(var_df, info_map, ticker_meta, ascending, n=20):
    html = ""
    cnt  = 0
    for tk, row in var_df.sort_values("var_1d", ascending=ascending).iterrows():
        if cnt >= n: break
        info   = info_map.get(tk, {})
        meta   = ticker_meta.get(tk, {})
        name   = info.get("name", tk)
        borsa  = meta.get("borsa", "—")
        indice = meta.get("indice", "—")
        cap    = fmt_cap(info.get("mktcap"))
        bg     = ("#fff8f8" if ascending else "#f8fff8") if cnt % 2 == 0 else "#ffffff"
        html += (f'<tr style="background:{bg}">'
                 f'<td style="{TD};text-align:center;color:#aaa">{cnt+1}</td>'
                 f'<td style="{TD}">{ylink(tk,name)}<br>'
                 f'<span style="color:#bbb;font-size:10px">{tk}</span></td>'
                 f'<td style="{TD};color:#555">{borsa}</td>'
                 f'<td style="{TD};color:#555;font-size:11px">{indice}</td>'
                 f'<td style="{TD};text-align:right">{fmt_price(row["prezzo"])}</td>'
                 f'<td style="{TD};text-align:right">{fmt_pct(row["var_1d"])}</td>'
                 f'<td style="{TD};text-align:right">{fmt_pct(row["var_7d"])}</td>'
                 f'<td style="{TD};text-align:right">{fmt_pct(row["var_30d"])}</td>'
                 f'<td style="{TD};text-align:right;color:#555">{cap}</td></tr>')
        cnt += 1
    return html

def _rows_weekly(var_df, info_map, ticker_meta, ascending, n=20):
    html = ""
    cnt  = 0
    for tk, row in var_df.sort_values("var_settimana", ascending=ascending).iterrows():
        if cnt >= n: break
        info   = info_map.get(tk, {})
        meta   = ticker_meta.get(tk, {})
        name   = info.get("name", tk)
        borsa  = meta.get("borsa", "—")
        indice = meta.get("indice", "—")
        cap    = fmt_cap(info.get("mktcap"))
        bg     = ("#fff8f8" if ascending else "#f8fff8") if cnt % 2 == 0 else "#ffffff"
        html += (f'<tr style="background:{bg}">'
                 f'<td style="{TD};text-align:center;color:#aaa">{cnt+1}</td>'
                 f'<td style="{TD}">{ylink(tk,name)}<br>'
                 f'<span style="color:#bbb;font-size:10px">{tk}</span></td>'
                 f'<td style="{TD};color:#555">{borsa}</td>'
                 f'<td style="{TD};color:#555;font-size:11px">{indice}</td>'
                 f'<td style="{TD};text-align:right">{fmt_price(row["prezzo"])}</td>'
                 f'<td style="{TD};text-align:right">{fmt_pct(row["var_settimana"])}</td>'
                 f'<td style="{TD};text-align:right;color:#555">{cap}</td></tr>')
        cnt += 1
    return html

TBL = '<table style="width:100%;border-collapse:collapse;background:white;box-shadow:0 1px 6px rgba(0,0,0,0.08)">'
STYLE = "font-family:Arial,sans-serif;max-width:980px;margin:auto;padding:20px;color:#222;background:#fafafa"

def build_daily(var_df, info_map, ticker_meta, indici_info, data_rif):
    ds = data_rif.strftime("%d/%m/%Y")
    gn = data_rif.strftime("%A %d %B %Y")
    return f"""<html><body style="{STYLE}">
  <div style="background:#1a1a2e;color:white;padding:22px 24px;border-radius:10px;margin-bottom:18px">
    <h2 style="margin:0 0 4px;font-size:21px">📊 Report Mercati Azionari — {ds}</h2>
    <p style="margin:0;opacity:0.65;font-size:13px">{gn} · ~3.200 titoli monitorati</p>
  </div>
  <h3 style="margin:0 0 10px;font-size:14px;color:#444">Andamento indici principali</h3>
  {indici_bar(indici_info)}
  <h3 style="color:#cc0000;margin:24px 0 8px;font-size:16px">📉 Top 20 Peggiori del giorno</h3>
  {TBL}{THEAD_DAILY}<tbody>{_rows_daily(var_df,info_map,ticker_meta,True)}</tbody></table>
  <br>
  <h3 style="color:#007700;margin:28px 0 8px;font-size:16px">📈 Top 20 Migliori del giorno</h3>
  {TBL}{THEAD_DAILY}<tbody>{_rows_daily(var_df,info_map,ticker_meta,False)}</tbody></table>
  <p style="color:#bbb;font-size:11px;text-align:center;margin-top:28px">
    Dati: Yahoo Finance · Solo scopo informativo · Non costituisce consulenza finanziaria
  </p>
</body></html>"""

def build_weekly(var_df, info_map, ticker_meta, indici_info, wstart, wend):
    return f"""<html><body style="{STYLE}">
  <div style="background:#2c3e50;color:white;padding:22px 24px;border-radius:10px;margin-bottom:18px">
    <h2 style="margin:0 0 4px;font-size:21px">📊 Report Settimanale — {wstart.strftime('%d/%m')}–{wend.strftime('%d/%m/%Y')}</h2>
    <p style="margin:0;opacity:0.65;font-size:13px">Variazione sui 5 giorni di borsa · ~3.200 titoli monitorati</p>
  </div>
  <h3 style="margin:0 0 10px;font-size:14px;color:#444">Andamento indici nella settimana</h3>
  {indici_bar(indici_info)}
  <h3 style="color:#cc0000;margin:24px 0 8px;font-size:16px">📉 Top 20 Peggiori della settimana</h3>
  {TBL}{THEAD_WEEKLY}<tbody>{_rows_weekly(var_df,info_map,ticker_meta,True)}</tbody></table>
  <br>
  <h3 style="color:#007700;margin:28px 0 8px;font-size:16px">📈 Top 20 Migliori della settimana</h3>
  {TBL}{THEAD_WEEKLY}<tbody>{_rows_weekly(var_df,info_map,ticker_meta,False)}</tbody></table>
  <p style="color:#bbb;font-size:11px;text-align:center;margin-top:28px">
    Dati: Yahoo Finance · Solo scopo informativo · Non costituisce consulenza finanziaria
  </p>
</body></html>"""

# ── Invio email ────────────────────────────────────────────────────────────────

def invia(subject, html):
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = EMAIL_MITTENTE
    msg["To"]      = EMAIL_DESTINATARIO
    msg.attach(MIMEText(html, "html"))
    ctx = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=ctx) as s:
        s.login(EMAIL_MITTENTE, APP_PASSWORD)
        s.sendmail(EMAIL_MITTENTE, EMAIL_DESTINATARIO, msg.as_string())
    print(f"✅ Email inviata: {subject}")

# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    now    = datetime.now(timezone.utc)
    lunedi = now.weekday() == 0
    print(f"📅 {now.strftime('%A %d/%m/%Y %H:%M UTC')}")
    print(f"📬 Modalità: {'settimanale (lunedì)' if lunedi else 'giornaliera'}\n")

    # 1. Paniere
    all_tickers, ticker_meta = build_universe()

    # 2. Prezzi
    print()
    close = scarica_prezzi(all_tickers, giorni=45)
    print(f"   Titoli con dati validi: {close.shape[1]}/{len(all_tickers)}\n")

    # 3. Indici
    print("⬇️  Download indici...")
    indici_info = scarica_indici()

    # 4. Variazioni
    if lunedi:
        var_df = calc_variazioni_settimanali(close)
        sort_col = "var_settimana"
    else:
        var_df = calc_variazioni(close)
        sort_col = "var_1d"
    print(f"   Variazioni calcolate: {len(var_df)} titoli\n")

    # 5. Info solo per i 40 finalisti
    top40 = (var_df.sort_values(sort_col).head(20).index.tolist() +
             var_df.sort_values(sort_col, ascending=False).head(20).index.tolist())
    top40 = list(dict.fromkeys(top40))
    print()
    info_map = scarica_info_batch(top40)

    # 6. Build + send
    print("\n📧 Costruzione e invio email...")
    if lunedi:
        wend   = now - timedelta(days=3)
        wstart = wend - timedelta(days=4)
        html   = build_weekly(var_df, info_map, ticker_meta, indici_info, wstart, wend)
        invia(f"📊 Report Settimanale {wstart.strftime('%d/%m')}–{wend.strftime('%d/%m/%Y')}", html)
    else:
        data_rif = now - timedelta(days=1)
        if data_rif.weekday() == 6:
            data_rif -= timedelta(days=2)
        html = build_daily(var_df, info_map, ticker_meta, indici_info, data_rif)
        invia(f"📊 Report Mercati {data_rif.strftime('%d/%m/%Y')}", html)

if __name__ == "__main__":
    main()
