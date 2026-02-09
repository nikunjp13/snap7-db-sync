"""
Example Use Case: Industrial Tank Level Control
-----------------------------------------------
PURPOSE:
This script illustrates the 'snap7-db-sync' library's ability to facilitate
high-level process control and data acquisition. It is designed to simplify
the creation of live dashboards, data analysis pipelines, and large-scale
data sampling from Siemens S7 PLCs.
It uses background PLC polling and shared memory to handle data transfer,
allowing the main logic to focus on process control.

⚠️ SAFETY NOTICE:
This code is for ILLUSTRATION PURPOSES ONLY. Traditional industrial systems
require robust safety measures, hardware interlocks, and redundancy checks.
This library is not a replacement for deterministic, safety-rated PLC logic.
Critical safety functions (e.g., emergency stops, over-pressure protection)
should always be handled directly within the PLC hardware and safety-rated
subsystems.

The example considers following scenario:
1. Monitors a 'Power' state and initializes the system.
2. Monitors 'Tank_level' and 'Pump_On' status.
3. Automatically controls the 'Pump_ON' command and 'Release_valve' position.
"""

import json
import time
from multiprocessing.shared_memory import SharedMemory
from snap7_db_sync import Snap7DBSync


def read_single_variable(variable_key, shared_mem, shared_mem_size):
    """Retrieves a single value from the shared memory JSON buffer."""
    raw = bytes(shared_mem.buf[:shared_mem_size]).rstrip(b"\x00")
    if raw:
        data = json.loads(raw)
        return data.get(variable_key)
    return None


def read_multiple_variables(variable_keys, shared_mem, shared_mem_size):
    """Retrieves a subset of variables from shared memory as a dictionary."""
    raw = bytes(shared_mem.buf[:shared_mem_size]).rstrip(b"\x00")
    if raw:
        data = json.loads(raw)
        if isinstance(variable_keys, list):
            return {key: data[key] for key in variable_keys if key in data}
    return None


def read_all_variables(shared_mem, shared_mem_size):
    """Returns the entire PLC Data Block image as a dictionary."""
    raw = bytes(shared_mem.buf[:shared_mem_size]).rstrip(b"\x00")
    return json.loads(raw) if raw else None


# --- Configuration ---
PLC_IP = "192.168.0.100"
DB_NUMBER = 100
BLUEPRINT_FILE = "demo_db_blueprint1.txt" # Right click on DB copy as text -> .txt
BLUEPRINT_FILE2 = "demo_db_blueprint2.txt" # Inside the DB select all -> txt
SHM_NAME = "shared_plc_data"
SHM_SIZE = 2048  # Must match the size defined in Snap7DBSync
CYCLE_TIME_MS = 20

# 1. Initialize the Sync Engine
plc_comm = Snap7DBSync(
    ip_addr=PLC_IP,
    db_num=DB_NUMBER,
    db_bluprint_txt=BLUEPRINT_FILE,
    shm_name=SHM_NAME,
    shm_size=SHM_SIZE
)

# 2. Establish Connection and Start Background Thread
connection = plc_comm.connect()
time.sleep(0.1)  # Brief pause to allow thread stabilization

if connection:
    plc_comm.start_logging(cycle_time_ms=CYCLE_TIME_MS)
    time.sleep(0.1)

    # 3. Access the Shared Memory segment created by the library
    shared_mem = SharedMemory(name=SHM_NAME, create=False)

    # Initial System Check: Ensure the plant is powered on
    power_state = read_single_variable("Power", shared_mem, SHM_SIZE)
    if power_state is not True:
        print("System Power is OFF. Sending Power_ON command...")
        plc_comm.write_to_plc({"Power_ON": True})
        time.sleep(0.5)

    print("Control Loop Active. Monitoring Tank Levels...")

    try:
        while True:
            # 4. Read process variables
            plant_state = read_multiple_variables(["Tank_level", "Pump_On"], shared_mem=shared_mem,
                                                  shared_mem_size=SHM_SIZE)

            # 5. Process Control Logic
            if plant_state["Tank_level"] < 50 and not plant_state["Pump_On"]:
                # Fill the tank
                plc_comm.write_to_plc({"Pump_ON": True, "Release_valve": 0})
            elif 50 < plant_state["Tank_level"] < 75:
                # Maintenance range - stop filling
                plc_comm.write_to_plc({"Pump_ON": False})
            elif plant_state["Tank_level"] > 75:
                # High level - Emergency drain
                plc_comm.write_to_plc({"Pump_ON": False, "Release_valve": 25})

            # Control loop frequency (independent of PLC polling frequency)
            time.sleep(5)

    except KeyboardInterrupt:
        print("\nStopping control loop...")
        plc_comm.close_connection()
