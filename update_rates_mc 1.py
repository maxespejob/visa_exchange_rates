from array import ArrayType
import requests
from typing import List
import json
import time, random
from datetime import datetime
import sqlalchemy
from bs4 import BeautifulSoup
import warnings
import pandas as pd
import numpy as np
import concurrent.futures
import Module.Logs.logs as log
from Module.Persistence.connection import connect_to_postgreSQL as bdpostgre
from Module.Persistence.connection import connect_to_s3 as s3
import pathlib, os
from dotenv import load_dotenv


class ExchangeRates:
    """Class used for obtain exchange rates of the day.

    settings are obtained from S3 repository via request to bucket.
    Params:
        info_date (str): date of the day to extract exchange rate.
    """

    warnings.simplefilter(action="ignore", category=FutureWarning)

    def __init__(self, info_date, **kwargs) -> None:
        load_dotenv()
        self.structured = os.getenv("INTELICA_DEVOPS")
        info_settings = s3().get_object(
            self.structured, "app-interchange/config/proxy_settings.json", "", False
        )
        settings = json.loads(info_settings["Body"].read())
        self.HEADERS_MASTERCARD = settings.get("header_settings").get(
            "HEADERS_MASTERCARD"
        )
        self.brand_mastercard = "MasterCard"
        self.VERIFY: bool = True

        if info_date is None:
            info_date = datetime.now().strftime("%m/%d/%Y")
            self.info_date = info_date
        else:
            info_date = info_date.strftime("%m/%d/%Y")
            self.info_date = info_date

        super().__init__(**kwargs)

    def get_currency_list_mastercard(self, proxy_list_mastercard) -> List:
        """List of exchange rates available on the date the process was executed of Mastercard"""
        s = requests.Session()
        header_list_mastercard = self.HEADERS_MASTERCARD
        header_list_mastercard["sec-fetch-mode"] = "navigate"
        header_list_mastercard["sec-fetch-site"] = "none"
        header_list_mastercard["purpose"] = "prefetch"

        # J.Cardenas: Added headers to avoid 403 error
        header_list_mastercard["user-agent"] = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36 Edg/145.0.0.0"
        )
        header_list_mastercard["accept-language"] = (
            "es,es-ES;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6,es-PE;q=0.5"
        )
        response = None
        for proxy in proxy_list_mastercard:
            proxies = {"http": proxy["proxy"], "https": proxy["proxy"]}
            try:
                response = s.get(
                    "https://www.mastercard.com/settlement/currencyrate/settlement-currencies",
                    headers=header_list_mastercard,
                    proxies=proxies,
                    verify=self.VERIFY,
                    timeout=3,
                )

                if response.status_code == 200:
                    break
            except requests.RequestException:
                continue

        if not response or response.status_code != 200:
            return "error: proxies doesnt work"

        try:
            data = json.loads(response.content)
        except json.JSONDecodeError:
            return f"error"

        currency_list = [currency["alphaCd"] for currency in data["data"]["currencies"]]

        results_mastercard = []
        for type_1 in currency_list:
            for type_2 in currency_list:
                if type_1 != type_2:
                    results_mastercard.append([type_1, type_2])

        return results_mastercard

    def get_proxies_funcionales(self, proxy_list_mastercard) -> List:
        """List of exchange rates available on the date the process was executed of Mastercard"""
        s = requests.Session()
        header_list_mastercard = self.HEADERS_MASTERCARD
        header_list_mastercard["sec-fetch-mode"] = "navigate"
        header_list_mastercard["sec-fetch-site"] = "none"
        header_list_mastercard["purpose"] = "prefetch"

        # J.Cardenas: Added headers to avoid 403 error
        header_list_mastercard["user-agent"] = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36 Edg/138.0.0.0"
        )
        header_list_mastercard["accept-language"] = (
            "es,es-ES;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6,es-PE;q=0.5"
        )

        proxies_bloqueados = []
        proxies_funcionales = []

        for proxy in proxy_list_mastercard:
            proxies = {"http": proxy["proxy"], "https": proxy["proxy"]}
            try:
                resp = s.get(
                    "https://www.mastercard.com/settlement/currencyrate/settlement-currencies",
                    headers=header_list_mastercard,
                    proxies=proxies,
                    verify=self.VERIFY,
                    timeout=3,
                )

                if resp.status_code == 200:
                    proxies_funcionales.append(proxy)
                else:
                    proxies_bloqueados.append(proxy)

            except requests.RequestException:
                proxies_bloqueados.append(proxy)

        return proxies_funcionales, proxies_bloqueados

    def exchange_conversor_mastercard(self, list_mastercard, proxy_element) -> List:
        """Exchange Converter of Mastercard

        Args:
            list_mastercard (list): list of currency for exchange
            proxy_element (list): list of proxy element
        Returns:
            result_mastercard_list (list):  list of proxy element.
            proxy_element (list): result of the search.

        """
        proxy = proxy_element["proxy"]
        proxy_dict = {"http": proxy, "https": proxy}

        date_mastercard = datetime.strptime(self.info_date, "%m/%d/%Y").strftime(
            "%Y-%m-%d"
        )
        s = requests.Session()
        if list_mastercard != []:
            result_mastercard_list = []
            for i in list_mastercard:
                type_1 = i[0]
                type_2 = i[1]

                mastercard_params = {
                    "exchange_date": str(date_mastercard),
                    "transaction_currency": f"{type_1}",
                    "cardholder_billing_currency": f"{type_2}",
                    "bank_fee": "0",
                    "transaction_amount": "1",
                }

                try:
                    mastercard = s.get(
                        "https://www.mastercard.com/marketingservices/public/mccom-services/currency-conversions/conversion-rates",
                        params=mastercard_params,
                        headers=self.HEADERS_MASTERCARD,
                        verify=self.VERIFY,
                        proxies=proxy_dict,
                        timeout=3,
                    )
                    response = json.loads(mastercard.content)
                    value = float(
                        str(response["data"]["conversionRate"]).replace(",", "")
                    )

                except Exception:
                    value = np.nan
                    proxy_element["status"] = "inactive"

                data = (type_1, type_2, value)
                result_mastercard_list.append(data)

            return result_mastercard_list, proxy_element
        else:
            return [], proxy_element

    def run_full_mastercard(self, available_list, proxy_list) -> pd.DataFrame:
        """Performer that requests the information from the MASTERCARD page of the available exchange rates that are an input to this function along with a proxy list

        Args:
           available_list (list): list of currency.
           proxy_list (list): list of available proxy
        Returns:
            df_conversor_mastercard (Dataframe): data extracted with the exchange rates.
            inactive_proxies (list): list of inactive proxys.


        """

        active_proxy_list = [i for i in proxy_list if i["status"] == "active"]

        divider = np.array_split(available_list, len(active_proxy_list))

        with concurrent.futures.ProcessPoolExecutor() as executor:
            results_m = executor.map(
                self.exchange_conversor_mastercard, divider, active_proxy_list
            )

        results_mastercard = []
        inactive_proxies = []
        for i in results_m:
            if i[1]["status"] == "inactive":
                inactive_proxies.append(i[1])
            for a in i[0]:
                results_mastercard.append(a)

        header_date = datetime.strptime(str(self.info_date), "%m/%d/%Y").strftime(
            "%Y-%m-%d"
        )
        all_converter_changes_mastercard = [
            [type_1, type_2, value, header_date, self.brand_mastercard]
            for (type_1, type_2, value) in results_mastercard
        ]

        df_conversor_mastercard = pd.DataFrame(
            all_converter_changes_mastercard,
            columns=["currency_from", "currency_to", "exchange_value", "date", "brand"],
        )
        df_conversor_mastercard = df_conversor_mastercard.reindex(
            columns=["date", "brand", "currency_from", "currency_to", "exchange_value"]
        )
        return df_conversor_mastercard, inactive_proxies

    def combiner_process(self, log_name) -> pd.DataFrame:
        """Executor of the process for the generation of all information on mastercard exchange rates considering reprocesses

        Args:
           log_name (str): name of log file.
        Returns:
           all_data (pd.Dataframe): processed data
        """
        module_name = "EXCHANGE RATE"
        info_settings_mastercard = s3().get_object(
            self.structured, "app-interchange/config/proxy_settings.json", "", False
        )
        settings_mastercard = json.loads(info_settings_mastercard["Body"].read())
        proxy_list_mastercard = settings_mastercard.get("proxy_settings").get(
            "proxy_list_mastercard"
        )
        all_mastercard = self.get_currency_list_mastercard(proxy_list_mastercard)
        # proxies_funcionales, proxies_bloqueados = self.get_proxies_funcionales(proxy_list_mastercard)

        log.logs().exist_file(
            "EXCHANGE_RATE",
            "INTELICA",
            "MASTERCARD",
            log_name,
            "PROCESSING EXCHANGE RATES",
            "INFO",
            "amount of exchange rates to processing : " + str(len(all_mastercard)),
            module_name,
        )

        mastercard_process = self.run_full_mastercard(
            all_mastercard, proxy_list_mastercard
        )
        mastercard_data = pd.DataFrame(mastercard_process[0])

        inactive_proxy_mastercard = mastercard_process[1]

        for i in proxy_list_mastercard:
            for r in inactive_proxy_mastercard:
                if i["proxy"] == r["proxy"]:
                    i["status"] = r["status"]

        mastercard_reprocess = mastercard_data.query(
            "exchange_value.isnull()", engine="python"
        )

        counter_mastercard = 0
        initial_proxy_list_mastercard = proxy_list_mastercard
        r_analyze_proxy_mastercard = [
            proxy
            for proxy in initial_proxy_list_mastercard
            if proxy["status"] == "active"
        ]
        original_info_settings_mastercard = s3().get_object(
            self.structured, "app-interchange/config/proxy_settings.json", "", False
        )
        original_settings_mastercard = json.loads(
            original_info_settings_mastercard["Body"].read()
        )
        original_proxy_list_mastercard = original_settings_mastercard.get(
            "proxy_settings"
        ).get("proxy_list_mastercard")
        quality_proxies_mastercard = (
            len(r_analyze_proxy_mastercard) / len(original_proxy_list_mastercard)
        ) * 100
        quantity_missing_mastercard = (
            len(mastercard_reprocess) / len(mastercard_data)
        ) * 100

        if mastercard_reprocess.empty:
            log.logs().exist_file(
                "EXCHANGE_RATE",
                "INTELICA",
                "MASTERCARD",
                log_name,
                "NO UNPROCESSED TYPES HAVE BEEN DETECTED",
                "INFO",
                "amount of exchange rates to reprocess : "
                + str(len(mastercard_reprocess)),
                module_name,
            )
        else:
            log.logs().exist_file(
                "EXCHANGE_RATE",
                "INTELICA",
                "MASTERCARD",
                log_name,
                f"PROCESS IDENTIFIES A QUALITY OF PROXIES OF THE {str(round(quality_proxies_mastercard, 2))}% AND A NUMBER OF MISSING OF THE {str(len(mastercard_reprocess))} ({str(round(quantity_missing_mastercard, 2))}%)",
                "INFO",
                f"Analyzing to respond to the process",
                module_name,
            )
            if quality_proxies_mastercard < 50:
                if quantity_missing_mastercard <= 3:
                    log.logs().exist_file(
                        "EXCHANGE_RATE",
                        "INTELICA",
                        "MASTERCARD",
                        log_name,
                        f"PROCESS NEEDS MINIMUM 50% OF PROXIES TO CONTINUE, THERE ARE ONLY {len(r_analyze_proxy_mastercard)} PROXIES ENABLED FOR REPROCESSING",
                        "INFO",
                        f"Giving a 20 minutes timeout to restart proxies",
                        module_name,
                    )
                    time.sleep(1200)

                    log.logs().exist_file(
                        "EXCHANGE_RATE",
                        "INTELICA",
                        "MASTERCARD",
                        log_name,
                        f"PROCESS WITH PROXIES ENABLED",
                        "INFO",
                        f"Starting reprocess, time out completed",
                        module_name,
                    )
                    starting_proxy_list_mastercard = original_proxy_list_mastercard
                else:
                    log.logs().exist_file(
                        "EXCHANGE_RATE",
                        "INTELICA",
                        "MASTERCARD",
                        log_name,
                        f"PROCESS NEEDS MINIMUM 50% OF PROXIES TO CONTINUE, THERE ARE ONLY {len(r_analyze_proxy_mastercard)} PROXIES ENABLED FOR REPROCESSING",
                        "INFO",
                        f"Giving a 25 minutes timeout to restart proxies",
                        module_name,
                    )
                    time.sleep(1500)

                    log.logs().exist_file(
                        "EXCHANGE_RATE",
                        "INTELICA",
                        "MASTERCARD",
                        log_name,
                        f"PROCESS WITH PROXIES ENABLED",
                        "INFO",
                        f"Starting reprocess, time out completed",
                        module_name,
                    )
                    starting_proxy_list_mastercard = original_proxy_list_mastercard
            else:
                if quantity_missing_mastercard <= 3:
                    log.logs().exist_file(
                        "EXCHANGE_RATE",
                        "INTELICA",
                        "MASTERCARD",
                        log_name,
                        f"PROCESS HAS THE MINIMUM 50% OF PROXIES TO CONTINUE, THERE ARE {len(r_analyze_proxy_mastercard)} PROXIES ENABLED FOR REPROCESSING",
                        "INFO",
                        f"Giving a 10 minutes timeout to restart proxies",
                        module_name,
                    )
                    time.sleep(600)
                    log.logs().exist_file(
                        "EXCHANGE_RATE",
                        "INTELICA",
                        "MASTERCARD",
                        log_name,
                        f"PROCESS WITH PROXIES ENABLED",
                        "INFO",
                        f"Starting reprocess, time out completed",
                        module_name,
                    )
                    starting_proxy_list_mastercard = original_proxy_list_mastercard

                else:
                    log.logs().exist_file(
                        "EXCHANGE_RATE",
                        "INTELICA",
                        "MASTERCARD",
                        log_name,
                        f"PROCESS HAS THE MINIMUM 50% OF PROXIES TO CONTINUE, THERE ARE {len(r_analyze_proxy_mastercard)} PROXIES ENABLED FOR REPROCESSING",
                        "INFO",
                        f"Giving a 25 minutes timeout to restart proxies",
                        module_name,
                    )
                    time.sleep(1500)

                    log.logs().exist_file(
                        "EXCHANGE_RATE",
                        "INTELICA",
                        "MASTERCARD",
                        log_name,
                        f"Process with proxies enabled",
                        "INFO",
                        f"Starting reprocess, time out completed",
                        module_name,
                    )
                    starting_proxy_list_mastercard = original_proxy_list_mastercard

        while not mastercard_reprocess.empty:
            counter_mastercard += 1
            r_available_proxy_mastercard = [
                proxy
                for proxy in starting_proxy_list_mastercard
                if proxy["status"] == "active"
            ]

            if len(r_available_proxy_mastercard) == 0:
                missing_exchange_rates_mc = len(mastercard_reprocess)
                log.logs().exist_file(
                    "EXCHANGE_RATE",
                    "INTELICA",
                    "MASTERCARD",
                    log_name,
                    "PROCESS UNABLE TO CONTINUE WITH 0 PROXIES ENABLED FOR REPROCESSING",
                    "ERROR",
                    f"Finishing obtaining exchange rates from MASTERCARD, missing {missing_exchange_rates_mc} exchange rates",
                    module_name,
                )
                break

            log.logs().exist_file(
                "EXCHANGE_RATE",
                "INTELICA",
                "MASTERCARD",
                log_name,
                "COUNTER REPROCESSING UNPROCESSED EXCHANGE RATES: "
                + str(int(counter_mastercard)),
                "INFO",
                "amount of exchange rates to reprocess : "
                + str(len(mastercard_reprocess))
                + f" with {len(r_available_proxy_mastercard)} active proxies",
                module_name,
            )
            reprocess_mastercard_data = mastercard_reprocess[
                ["currency_from", "currency_to"]
            ].values.tolist()

            mastercard_r = self.run_full_mastercard(
                reprocess_mastercard_data, r_available_proxy_mastercard
            )
            mastercard_r_data = pd.DataFrame(mastercard_r[0])
            r_inactive_proxy_mastercard = mastercard_r[1]

            if len(r_inactive_proxy_mastercard) > 0:
                for i in starting_proxy_list_mastercard:
                    for r in r_inactive_proxy_mastercard:
                        if i["proxy"] == r["proxy"]:
                            i["status"] = r["status"]

            for index, row in mastercard_r_data.iterrows():
                a = row["currency_from"]
                b = row["currency_to"]
                value = row["exchange_value"]
                mastercard_data.loc[
                    (mastercard_data["currency_from"] == a)
                    & (mastercard_data["currency_to"] == b),
                    "exchange_value",
                ] = value

            mastercard_reprocess = mastercard_data.query(
                "exchange_value.isnull()", engine="python"
            )
        log.logs().exist_file(
            "EXCHANGE_RATE",
            "INTELICA",
            "MASTERCARD",
            log_name,
            "PROCESSING EXCHANGE RATES",
            "INFO",
            "completed",
            module_name,
        )
        frames = [mastercard_data]
        all_data = pd.concat(frames, ignore_index=True)

        return all_data

    def updater_process(self) -> None:
        """Process that connects to the database and updates the exchange rates."""
        module_name = "EXCHANGE RATE"
        log_name = log.logs().new_log(
            "EXCHANGE_RATE",
            "",
            "INTELICA",
            "GET MASTERCARD EXCHANGE RATES",
            "SYSTEM",
            module_name,
        )

        date_input = datetime.strptime(str(self.info_date), "%m/%d/%Y").strftime(
            "%Y-%m-%d"
        )

        log.logs().exist_file(
            "EXCHANGE_RATE",
            "INTELICA",
            "MASTERCARD",
            log_name,
            "GETTING EXCHANGE RATES OF THE DATE " + str(date_input),
            "INFO",
            "in process",
            module_name,
        )

        try:
            df = pd.DataFrame(self.combiner_process(log_name))
            check_codes = pd.DataFrame(bdpostgre().select("operational.m_currency"))

            for index, row in check_codes.iterrows():
                a = row["currency_alphabetic_code"]
                b = row["currency_numeric_code"]

                df.loc[
                    (df["currency_from"] == a),
                    "currency_from_code",
                ] = b
                df.loc[
                    (df["currency_to"] == a),
                    "currency_to_code",
                ] = b

            file_date_standard = datetime.strptime(self.info_date, "%m/%d/%Y")
            file_date = file_date_standard.strftime("%Y%m%d")
            file_date_format = file_date_standard.strftime("%Y-%m-%d")
            execution_detail = int(float(datetime.now().timestamp()))
            file_name = f"{file_date}_{execution_detail}_mastercard"
            local_route = f"FILES/EXCHANGE_RATE/"
            pathlib.Path(local_route).mkdir(parents=True, exist_ok=True)
            s3_route = f"EXCHANGE_RATE/{file_name}.parquet"
            local_file_parquet = f"{local_route}{file_name}.parquet"
            df.to_parquet(local_file_parquet)
            structured = os.getenv("STRUCTURED_BUCKET")
            upload = s3().upload_object(structured, local_file_parquet, s3_route)
            db = bdpostgre().prepare_engine()
            db.execution_options(autocommit=False)
            table_new = f"tmp_exchange_rate_{file_name}"
            schem = "temporal"
            tran = None
            df = pd.read_parquet(
                path=local_file_parquet, engine="fastparquet", storage_options=None
            )
            df.index = np.arange(1, len(df) + 1)
            df["app_id"] = df.index
            df["app_type_file"] = "EXCHANGE_RATE"
            df["app_processing_date"] = df["date"]
            list_of_columns = list(df.columns)
            list_of_columns = ",".join(list_of_columns)

            rows_inserted = bdpostgre().insert_from_dataframe(
                table_new,
                schem,
                df,
                if_exists="replace",
                dtype={
                    "date": sqlalchemy.DateTime,
                    "exchange_value": sqlalchemy.Numeric,
                    "app_processing_date": sqlalchemy.Date,
                },
            )
            check_reprocces = bdpostgre().select(
                "OPERATIONAL.DH_EXCHANGE_RATE",
                f"""WHERE date = '{file_date_format}' AND brand = '{self.brand_mastercard}'""",
                "count(date)",
            )

            if check_reprocces[0]["count"] > 1:
                log.logs().exist_file(
                    "EXCHANGE_RATE",
                    "INTELICA",
                    "MASTERCARD",
                    log_name,
                    "THE INFORMATION HAS ALREADY BEEN LOADED WITH THAT DATE",
                    "WARNING",
                    "clear the data with that date in the operational table",
                    module_name,
                )
                sql3 = f"drop table {schem}.{table_new};"
                bdpostgre().execute_block(sql3)

            else:
                sql2 = f"""
                    insert into operational.dh_exchange_rate({list_of_columns}) select {list_of_columns} from
                    {schem}.{table_new} """
                rs = 0
                rs = bdpostgre().execute_block(sql2, True)

                log.logs().exist_file(
                    "EXCHANGE_RATE",
                    "INTELICA",
                    "MASTERCARD",
                    log_name,
                    "UPDATING OPERATIONAL EXCHANGE RATE DATA TABLE",
                    "INFO",
                    "inserted rows : " + str(rs[1]),
                    module_name,
                )

                sql3 = f"drop table {schem}.{table_new};"
                bdpostgre().execute_block(sql3)

            log.logs().exist_file(
                "EXCHANGE_RATE",
                "INTELICA",
                "MASTERCARD",
                log_name,
                "GETTING EXCHANGE RATES OF THE DATE " + str(date_input),
                "INFO",
                "finished",
                module_name,
            )

        except Exception as e:
            log.logs().exist_file(
                "EXCHANGE_RATE",
                "INTELICA",
                "MASTERCARD",
                log_name,
                "GETTING EXCHANGE RATES OF THE DATE " + str(date_input),
                "ERROR",
                "A critical error has been detected, review or update the exchange rate configuration file: "
                + str(e),
                module_name,
            )
            log.logs().exist_file(
                "EXCHANGE_RATE",
                "INTELICA",
                "MASTERCARD",
                log_name,
                "GETTING EXCHANGE RATES OF THE DATE " + str(date_input),
                "ERROR",
                "Closing process with error",
                module_name,
            )
