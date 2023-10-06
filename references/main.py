import threading
from position_analysis import run_position_analysis
# from position_monitoring import run_position_monitoring
from position_option import run_position_option
# from order_executor import run_executor
from order_calculator import run_order_calculator
import os
import json
from dotenv import load_dotenv

load_dotenv()

def run_python(filepath):
    os.system('python ' + filepath)


if __name__ == "__main__":

    with open('input.json', 'r') as f:
        config = json.load(f)
    
    # try:
    #     os.remove(config['data_path'] + 'position_option.csv')
    #     os.remove(config['data_path'] + 'stock_position.csv')
    # except:
    #     pass

    # position_option_thread = threading.Thread(target=run_position_option, daemon=True)
    # position_option_thread.start()

    position_analysis_thread = threading.Thread(target=run_position_analysis, daemon=True)
    position_analysis_thread.start()

    order_calculator_thread = threading.Thread(target=run_order_calculator, daemon=True)
    order_calculator_thread.start()

    executor_thread = threading.Thread(target=run_python, args=('order_executor.py',), daemon=True)
    executor_thread.start()

    position_monitoring_thread = threading.Thread(target=run_python, args=('position_monitoring.py',), daemon=True)
    position_monitoring_thread.start()

    while(True):
        pass
