class Constants:
    QUERIES = {
        "INSERT_USER_CATV_HISTORY": "INSERT INTO api_catv_history(user_id,wallet_address,token_address,source_depth, "
                                    "distribution_depth,transaction_limit,from_date,to_date,logged_time,token_type) "
                                    "VALUES('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}');",
        "INSERT_USER_CATV_PATH_SEARCH": "INSERT INTO api_catv_path_history(user_id,address_from,address_to,depth, "
                                        "from_date,to_date,logged_time,token_type,min_tx_amount, "
                                        "limit_address_tx_count, token_address) "
                                        "VALUES('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}');",
        "SELECT_CATV_JOBS": "SELECT id, message, retries_remaining, created FROM api_catv_job_queue "
                                "WHERE retries_remaining > 0",
        "SELECT_CSV_CATV_JOBS": "SELECT id, message, retries_remaining, created FROM api_csv_catv_job_queue "
                                "WHERE retries_remaining > 0 ",
        "UPDATE_CATV_JOBS": "UPDATE api_catv_job_queue j1 SET retries_remaining = retries_remaining - 1 "
                                "WHERE j1.retries_remaining > 0 ",
        "UPDATE_CSV_CATV_JOBS": "UPDATE api_csv_catv_job_queue j1 SET retries_remaining = retries_remaining - 1 "
                                "WHERE j1.retries_remaining > 0 ",
    }

    ERROR_MESSAGES = {
        "MISSING_RESULTS": "Missing {} results for the wallet address within the date range specified"
    }

    # BITQUERY MAPPINGS
    NETWORK_CHAIN_MAPPING_FOR_QUERY = {
            "LUNC": "terra",
            "KLAY": "klaytn",
            "BSC": "bsc",
            "BNB": "binance",
            "TRX": "tron",
            "EOS": "eos",
            "XLM": "stellar",
            "XRP": "ripple",
            "LTC": "litecoin",
            "BCH": "bitcash",
            "ADA": "cardano",
            "DOGE": "dogecoin",
            "ZEC": "zcash",
            "DASH": "dash"  
    }

    NETWORK_CHAIN_MAPPING_FOR_RESPONSE = {
            "LUNC": "cosmos",
            "KLAY": "ethereum",
            "BSC": "ethereum",
            "BNB": "binance",
            "TRX": "tron",
            "EOS": "eos",
            "XLM": "stellar",
            "XRP": "ripple",
            "LTC": "bitcoin",
            "BCH": "bitcoin",
            "DOGE": "bitcoin",
            "ZEC": "bitcoin",
            "DASH": "bitcoin",
            "ADA": "cardano",        
    }

    GRAPHQL_CURRENCY_MAPPING = {
        "TRX": "TRX",
        "BNB": "BNB",
        "KLAY": "KLAY",
        "BSC": "BNB"
    }