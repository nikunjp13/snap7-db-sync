# Snap7 DB Sync
A high-performance bridge to synchronize Siemens S7 PLC Data Blocks (DB) into Python Shared Memory (SHM).

## 🚀 Key Features
**Zero-Config Parser**: Automatically calculates offsets and word-alignment from TIA Portal text exports (SCL or Table-copy).

**Shared Memory IPC**: Stores PLC data as a JSON string in a shared memory segment for fast access by other processes.

**Lifecycle Managed**: Shared memory is only allocated when connect() is called and is cleaned up properly on exit.

**Thread-Safe**: Uses internal locking to prevent crashes during simultaneous PLC read/write operations.

## 🎯 Ideal Use Cases

This library is engineered to bridge the gap between industrial hardware and modern data stacks. It is best suited for:

**High-Speed Data Acquisition**: Sampling hundreds of PLC tags at up to 50Hz for time-series analysis.

**Live Dashboards**: Feeding real-time process data into Streamlit, Plotly, or Flask applications via Shared Memory with minimal latency.

**Industry 4.0 / AI**: Providing clean JSON data streams for machine learning models, anomaly detection, and predictive maintenance.

## ✍️ Reading & Writing

The library is designed for high-performance bidirectional communication:

**High-Speed Reads**: The engine is optimized for continuous, reliable sampling of large Data Block (DB) structures.

**On-Demand Writes**: The library supports writing values back to the PLC (e.g., toggling a command bit or updating a process setpoint).

**IMPORTANT Safety Notice**: While writing is supported, it should be used cautiously. This library does not replace deterministic safety logic. Always ensure your PLC has internal failsafes and logic to handle unexpected values, out-of-range data, or potential communication loss.

## 🛠 Usage
### 1. Install library 
    pip install snap7-db-sync


### 2. System settings
Ensure PLC and PC running the libray are on same network and PLC is in RUN mode.

Make sure the DB to be synced is non optimized.

PUT/GET communications are enabled in PLC configuration.


### 3. Get your Blueprint
Copy your Data Block structure from TIA Portal (Right-click > "Copy as text" or "Select All" > "Copy") and save it as db_blueprint.txt.


### 4. Run the Sync
Python

    from snap7_db_sync import Snap7DBSync

    # Initialize and Connect
    sync = Snap7DBSync(
        ip_addr="192.168.0.1",
        db_num=10,
        db_bluprint_txt="db_blueprint.txt"
    )
    
    if sync.connect():
        # Start background polling at 20ms (50Hz)
        sync.start_logging(cycle_time_ms=20)

    # Write values back to PLC safely
    payload = {"Bool_Variable": True, "Int_Variable" : 1234, "Real_variable": 567.890}
    sync.write_to_plc(payload)
    # Clean up when done
    sync.close_connection()

💡 For a complete implementation reference, including how to read/write multiple variables and handle process logic, see the **examples/example_use.py** script. It demonstrates a real-world tank level control scenario using the library.

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.
