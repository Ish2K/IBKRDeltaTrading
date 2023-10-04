import pandas as pd

def calculate_option_delta(df):
    position_option = df.drop_duplicates()
    position_option = position_option[["time", "symbol", "contractRight", "lastTradeDateOrContractMonth", "strike", "delta"]]

    position_option['delta'] = position_option['delta'].astype(float)
    comparator = position_option.groupby(["symbol", "contractRight", "lastTradeDateOrContractMonth", "strike"]).agg({'time': 'max'}).reset_index()

    # get the delta of the latest record with unique symbol secType contractRight lastTradeDateOrContractMonth strike

    order_calculator = position_option.merge(comparator, on=['symbol', "contractRight",'time'], how='inner')

    # sum the delta of same symbol

    return order_calculator.groupby(['symbol']).agg({'delta': 'sum'}).reset_index()

def calculate_stock_position(df):
    stock_position = df.drop_duplicates()

    return stock_position.groupby(['symbol']).agg({'position': 'sum'}).reset_index()