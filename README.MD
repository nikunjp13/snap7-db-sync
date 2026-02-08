# Snap7 DB Sync
A high-performance bridge to synchronize Siemens S7 PLC Data Blocks (DB) into Python Shared Memory (SHM).

# 🚀 Key Features
Zero-Config Parser: Automatically calculates offsets and word-alignment from TIA Portal text exports (SCL or Table-copy).

Shared Memory IPC: Stores PLC data as a JSON string in a shared memory segment for fast access by other processes.

Lifecycle Managed: Shared memory is only allocated when connect() is called and is cleaned up properly on exit.

Thread-Safe: Uses internal locking to prevent crashes during simultaneous PLC read/write operations.

# 🛠 Usage
# 1. Get your Blueprint
Copy your Data Block structure from TIA Portal (Right-click > "Copy as text" or "Select All" > "Copy") and save it as db_blueprint.txt.

# Run the Sync
Python

    from snap7_db_sync import Snap7DBSync

# Initialize metadata
    sync = Snap7DBSync(
        ip_addr="192.168.0.1",
        db_num=10,
        db_bluprint_txt="db_blueprint.txt"
        )

# Connect (Allocates Shared Memory and connects to PLC)
    if sync.connect():
        # Start background logging at 50ms intervals
        sync.start_logging(cycle_time_ms=50)
        
        # Write values back to PLC safely
        sync.write_to_plc({"MySensorValue": 123.4})
        
        # Clean up when done
        sync.close_connection()

📄 License

This project is licensed under the MIT License - see the LICENSE file for details.