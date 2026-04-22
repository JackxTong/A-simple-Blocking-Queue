import dsp_kx as kx
import pandas as pd
import numpy as np
from datetime import datetime
import logging

from swaps_analytics.core.constants import AssetClass, EnvType
from swaps_analytics.core.currencies import CurrencyEnum
from swaps_analytics.core.databases.connection_utils import ConnectionDetailsBuilder
from swaps_analytics.core.utils.enum_utils import currency_to_region
from public_trade_matching.constants import PublicCols, RfqCols

class ExtractPublicTradeData:
    def __init__(self, start_date: datetime.date, end_date: datetime.date, currency: CurrencyEnum, env: EnvType):
        self.start_date = start_date
        self.end_date = end_date
        self.currency = currency
        self.region = currency_to_region.get(currency)
        self.gw_conn = ConnectionDetailsBuilder(AssetClass.RATES).set_env_type(env).build()
        
        self.LEG_CONSTRAINT = {
            CurrencyEnum.EUR: "*InterestRateSwap/EUR*",
            CurrencyEnum.GBP: "*InterestRateSwap/GBP*",
            CurrencyEnum.USD: "*SOFR*",
        }

    def get_rfq_data(self) -> pd.DataFrame:
        query = f"""
            {{[rates;startDate;endDate]
                rates[?; `.broadwaymetrics.{self.region}.RFQ;
                    .constr.WITHIN[`date; (startDate; endDate)],
                    .constr.FIELDLIKE[`market; "*IRS*"],
                    .constr.FIELDLIKE[`legInstrumentType; "{self.LEG_CONSTRAINT[self.currency]}"],
                    .constr.FBY[=;`revisionNumber;max;`requestId];
                    0b;
                    .fstat.FIELDLASTAS[`date; `{RfqCols.trade_date}],
                    .fstat.FIELDLASTAS[("p$"; `sourceTimestamp); `{RfqCols.trade_time}],
                    .fstat.FIELDLAST[`requestId`numLegs`regulatoryScope`legPricingConvention`legSize`legInstrumentMaturityDate`endReason]
                ]
            }}"""
        df = kx.q(query, self.gw_conn, self.start_date, self.end_date).pd().reset_index(drop=True)
        df[RfqCols.rfq_id] = df[RfqCols.rfq_id].str.decode("utf-8")
        df['currency'] = self.currency.value
        logging.info(f"Extracted {len(df)} raw RFQ legs.")
        return df

    def get_mifid_data(self) -> pd.DataFrame:
        query = """
            {[rates;startDate;endDate]
                rates[?; `.marketinsights.slough.propellantTrades;
                    .constr.WITHIN[`date; (startDate; endDate + 30)],
                    .constr.WITHIN[`postTradeDateTime; ("p"$startDate; "p"$(endDate + 1))],
                    .constr.FIELDLIKE[`cfiCode; "SR*"];
                    0b;
                    .istat.FIELDAS[`tradingDateTime; `publicTime],
                    .istat.FIELDAS[`notionalAmount; `publicSize],
                    .istat.FIELDAS[`price; `publicPrice],
                    .istat.FIELDAS[`source; `publicSource],
                    .istat.FIELDAS[`notionalCurrency; `currency],
                    .istat.FIELD[`drvExpiryDate]
                ]
            }"""
        df = kx.q(query, self.gw_conn, self.start_date, self.end_date).pd()
        df = df[(df['currency'] == self.currency.value) & (df['drvExpiryDate'].notna())]
        
        if df[PublicCols.size].dtype == 'O':
            df[PublicCols.size] = df[PublicCols.size].str.replace(',', '').astype(float)
            
        df[PublicCols.maturity_year] = pd.to_datetime(df['drvExpiryDate']).dt.year
        logging.info(f"Extracted {len(df)} MiFID trades.")
        return df

    def get_sdr_data(self) -> pd.DataFrame:
        query = f"""
            {{[rates;startDate;endDate]
                rates[?; `.sdr.pisc.1.SDR;
                    .constr.WITHIN[`date; (startDate; endDate + 20)],
                    .constr.WITHIN[`ExecutionTimestamp; ("p"$startDate; "p"$(endDate + 1))],
                    .constr.FIELD[`NotionalCurrencyLeg1; `{self.currency.value}],
                    .constr.FIELD[`ActionType; `NEW`CORRECT`REVI],
                    .constr.FIELD[`EventType; `Trade];
                    0b;
                    .istat.FIELDAS[`ExecutionTimestamp; `publicTime],
                    .istat.FIELDAS[`NotionalAmountLeg1; `publicSize],
                    .istat.FIELDAS[`ExpirationDate; `drvExpiryDate],
                    .istat.FIELD[`PriceNotation`AdditionalPriceNotation`PackageTransactionPrice`OtherPaymentAmount]
                ]
            }}"""
        df = kx.q(query, self.gw_conn, self.start_date, self.end_date).pd()
        
        # Consolidate SDR Pricing Logic
        df['sdrLegPrice'] = np.where(pd.notna(df["PriceNotation"]), 100 * df["PriceNotation"], np.nan)
        df['sdrLegPrice'] = np.where(pd.notna(df["AdditionalPriceNotation"]), 100 * df["AdditionalPriceNotation"], df['sdrLegPrice'])
        df['pkgPrice'] = np.where(pd.isna(df["PackageTransactionPrice"]), df["OtherPaymentAmount"], df["PackageTransactionPrice"])
        
        df[PublicCols.maturity_year] = pd.to_datetime(df['drvExpiryDate']).dt.year
        logging.info(f"Extracted {len(df)} SDR trades.")
        return df