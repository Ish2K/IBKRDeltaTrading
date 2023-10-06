import pandas as pd

def calculate_option_delta(df, config):
    position_option = df.drop_duplicates()
    position_option = position_option[["time", "symbol", "contractRight", "lastTradeDateOrContractMonth", "strike", "delta", "position"]]
    # position_option = position_option.groupby(['symbol']).agg({'delta': 'sum', 'position':'sum'}).reset_index()
    position_option["targe_delta"] = position_option.apply(lambda x: config['trading'][x['symbol']]['target_delta'] if (x['symbol'] in config['trading']) else 0, axis=1)
    position_option["idealPosition"] = round((position_option["targe_delta"] - position_option["delta"]) * position_option["position"] * 100)
    
    return position_option

def calculate_stock_position(df):
    stock_position = df.drop_duplicates()

    stock_position = stock_position.groupby(['symbol']).agg({'position': 'sum'}).reset_index()
    stock_position.rename(columns={'position': 'stock_position'}, inplace=True)

    return stock_position

def calculate_adjustment(order_calculator):

    order_calculator = order_calculator.groupby(['symbol']).agg({'idealPosition': 'sum', 'stock_position':'max'}).reset_index()
    order_calculator['adjustment'] = order_calculator['idealPosition'] - order_calculator['stock_position']
    
    return order_calculator