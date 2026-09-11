import importlib.util
import os
from pathlib import Path

FLOW_DIR = Path(__file__).resolve().parent
FLOW_PATH = FLOW_DIR / "customer_flow.py"

# Save the PNG in the same directory as the flow definition
os.chdir(FLOW_DIR)

spec = importlib.util.spec_from_file_location("customer_flow", FLOW_PATH)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

flow = module.customer_data_pipeline
flow.visualize()
