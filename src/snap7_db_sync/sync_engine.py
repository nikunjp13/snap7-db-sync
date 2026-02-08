import snap7
import json
import time
import struct
import threading
import multiprocessing.shared_memory as shared_memory
import re

class Snap7DBSync:
    """
    A high performance bridge to synchronize Siemens S7 PLC Data Blocks (DB) into Python Shared Memory (SHM).

    This class requires TIA Portal blueprints (SCL or Table format) to build a memory map.
    The process of reading PLC data in a singular and cyclic read operation is done in the background.
    The data from PLC DB is then flashed into python shared memory in the form of JSON string.
    This Shared memory is then available for using across different processes.
    """
    def __init__(
            self,
            ip_addr: str,
            db_num: int,
            db_bluprint_txt: str,
            rack: int = 0,
            slot: int = 1,
            shm_name: str = "plc_shared_data",
            shm_size: int = 2048,
    ):
        """
        Initiates the metadata for the process and builds the memory map from the db blueprint file.

        :param ip_addr: str: IP address of the PLC.
        :param db_num: int: Non optimized datablock number of the PLC.
        :param db_bluprint_txt: str: Path to the .txt file containing the data structure of the intended DB.
        :param rack: int: PLC rack number from the TIA project. (Default: 0)
        :param slot: int: PLC slot number from the TIA project. (Default: 1)
        :param shm_name: str: Unique name/identifier for the Shared memory segment.
        :param shm_size: int: Allocation size of the Shared memory segment. (Default: 2048)
        """
        self.ip_addr = ip_addr
        self.rack = rack
        self.slot = slot
        self.db_num = db_num
        self.client = None
        self._last_connect_error: str | None = None
        self.running = False
        self.lock = threading.Lock()

        self.shm = None
        self.shm_name = shm_name
        self.shm_size = shm_size
        self._last_len = 0

        self.total_len, self.data_struct = self.parse_siemens_db(db_bluprint_txt)
        print("Total length: ", self.total_len)
        print("Data struct: ", self.data_struct)

    # internal static helper methods
    @staticmethod
    def parse_siemens_db(content):
        """
        Parses Siemens DB definitions from SCL or Table-copy formats.
        Handles automatic offset calculation and word-alignment rules for Standard (Non-Optimized) Data Blocks.

        :param content: str: Path to the blueprint text file.
        :return: A tuple of (total_byte_length, data_structure_dictionary).
        """
        # Map Type -> (Size in Bytes, Alignment requirement)
        # Alignment: 1=Byte boundary, 2=Word (even byte) boundary
        types = {
            'bool': (1, 0), 'byte': (1, 1), 'word': (2, 2), 'int': (2, 2),
            'dword': (4, 2), 'dint': (4, 2), 'real': (4, 2), 'time': (4, 2)
        }

        data, byte_idx, bit_idx = {}, 0, 0
        try:
            with open(content, 'r') as f:
                file_content = f.read()
        except Exception as e:
            raise ValueError(f"Cannot read {content} ({e})")

        is_scl = "DATA_BLOCK" in file_content  # Detect format based on

        # Unified Regex: Matches "Name : Type" (SCL) OR "Name Type Offset" (TIA)
        # Group 1: Name, Group 2: Type, Group 3: Offset (Optional, TIA only)
        pattern = r'^\s*(\w+)(?:\s*:\s*|\s+)([A-Za-z]+)(?:\s*;|\s+([\d\.]+))?'

        for name, dtype, explicit_offset in re.findall(pattern, file_content, re.MULTILINE):
            dtype_key = dtype.lower()
            if dtype_key not in types: continue  # Skip unknown keywords (e.g. VERSION, STRUCT)

            size, align = types[dtype_key]

            if explicit_offset:  # --- TIA COPY FORMAT (Format 2) ---
                byte_idx, bit_idx = map(int, explicit_offset.split('.'))

            else:  # --- SCL FORMAT (Format 1) ---
                if dtype_key == 'bool':
                    if bit_idx > 7:  # Byte overflow
                        byte_idx += 1;
                        bit_idx = 0
                else:
                    if bit_idx > 0:  # Finish previous bool byte
                        byte_idx += 1;
                        bit_idx = 0
                    if align > 1 and byte_idx % 2 != 0:  # Word Alignment Padding
                        byte_idx += 1

            # Store Data
            data[name] = {'type': dtype, 'offset': byte_idx, 'bit': bit_idx, 'size': size}

            # Advance counters for SCL (or max size tracking for TIA)
            if dtype_key == 'bool':
                bit_idx += 1
            elif not explicit_offset:  # Only advance byte automatically if SCL
                byte_idx += size

        # Calculate Total Size (Max byte + last element size)
        last_item = max(data.values(), key=lambda x: x['offset'] + (0.1 * x['bit']))
        total_size = last_item['offset'] + (1 if last_item['type'].lower() == 'bool' else last_item['size'])

        return total_size, data

    @staticmethod
    def extract_data(data_byte, tag_dict) -> dict:
        """
        Extracts the variable values from the data received in bytes from the PLC into a dictionary.
        The keys represent the variable names in the db_blueprint_txt file.

        :param data_byte: byte: Data in byte from the PLC.
        :param tag_dict: dict: Dictionary of DB structure extracted from the db_blueprint_txt file.
        :return: A dictionary containing the variable names and their corresponding values.
        """
        results = {}

        # Map S7 types to Python Struct formats (Big-Endian '>' is required for PLC)
        # >h = Short (Int), >H = Unsigned Short (Word)
        # >i = Long (DInt), >I = Unsigned Long (DWord)
        # >f = Float (Real)
        type_map = {
            'int': '>h', 'word': '>H',
            'dint': '>i', 'dword': '>I',
            'real': '>f', 'time': '>i'
        }

        for name, meta in tag_dict.items():
            offset = meta['offset']
            dtype = meta['type'].lower()

            try:
                if dtype == 'bool':
                    # bool_data18 is at byte 2, bit 2 [cite: 4, 9]
                    # We read the byte at 'offset', shift right by 'bit', and mask with 1
                    byte_val = data_byte[offset]
                    results[name] = bool((byte_val >> meta['bit']) & 1)

                elif dtype == 'byte':
                    # Simple byte read, no unpacking needed
                    results[name] = data_byte[offset]

                elif dtype in type_map:
                    # Handle Multi-byte types (Int, Word, Real, etc.)
                    size = meta['size']
                    fmt = type_map[dtype]

                    # Slice the byte array and unpack
                    raw_val = struct.unpack(fmt, data_byte[offset:offset + size])[0]

                    # Optional: Round Real values to 4 decimals for cleaner output
                    if dtype == 'real':
                        raw_val = round(raw_val, 4)

                    results[name] = raw_val
            except IndexError:
                # Safety for cases where reading less data than defined
                results[name] = None
        return results

    # internal helper methods
    def _read_db(self) -> bytes:
        """
        Performs singular read operation from the PLC DB in bytes.

        :return: bytes: Data from PLC.
        """
        return self.client.db_read(self.db_num, 0, self.total_len)

    def _update_shared(self, payload: bytes) -> None:
        """
        Writes a byte payload into the Shared Memory buffer.

        If the new payload is smaller than the previous one, the difference is zero-filled to prevent stale data.
        Truncates if payload exceeds shm_size.
        Updates _self.last_len with the number of bytes written.

        :param payload: bytes: Data to be stored in shared memory. UTF-8 encoded bytes (typically a JSON string).
        """
        if not self.shm: return
        mv = self.shm.buf
        payload_len = len(payload)
        if payload_len > self.shm_size:
            payload = payload[:self.shm_size]
            payload_len = self.shm_size
        mv[:payload_len] = payload
        if self._last_len > payload_len:
            tail = self._last_len - payload_len
            mv[payload_len:payload_len + tail] = b"\x00"*tail
        self._last_len = payload_len

    # main logging method
    def _logging_loop(self, cycle_time_ms : int | float) -> None:
        """
        Main background loop that cyclically reads the PLC data, extracts values, and updates the shared memory.
        The loop runs when sel.running is True, handles transient snap7 communication errors with a small backoff
        followed by a quick reconnection when needed. Ensures pacing to the requested cycle time.

        :param cycle_time_ms: int | float: Targeted cycle time in milliseconds.
        """
        cycle_s = max(0.001, float(cycle_time_ms)/1000)
        backoff_s = 0.02
        last_values = None
        while self.running:
            t0 = time.perf_counter()
            try:
                with self.lock:
                    buf = self._read_db()
                    values = self.extract_data(buf, self.data_struct)

                # Only update shared memory if data actually changed
                if values != last_values:
                    payload = json.dumps(values, separators=(',', ':')).encode('utf-8')
                    self._update_shared(payload)
                    last_values = values
            except Exception as e:
                msg = str(e)
                if "Job pending" in msg or "CLI :" in msg:
                    time.sleep(backoff_s)
                    continue
                try:
                    self.client.disconnect()
                except Exception:
                    pass
                time.sleep(backoff_s)
                try:
                    self.client.connect(self.ip_addr, self.rack, self.slot)
                except Exception:
                    time.sleep(backoff_s)
                continue
            except Exception:
                time.sleep(backoff_s)
                continue

            elapsed = time.perf_counter() - t0
            sleep_s = cycle_s - elapsed
            if sleep_s > 0:
                time.sleep(sleep_s)

    # public methods
    def connect(self):
        """
        Allocates Shared Memory and establishes the PLC connection.
        Shared Memory is initialized here to ensure system resources are only reserved when the connection is active.
        Establishes a connection to the PLC.
        The snap7 client is created here and connection attempt is made.
        If attempt failed, the client is destroyed and reattempt is done once.

        :return: bool: True if connection is successful and SHM is allocated.
        """
        try:
            if self.shm is None:
                self.shm = shared_memory.SharedMemory(create=True, size=self.shm_size, name=self.shm_name)
            self.client = snap7.client.Client()
            time.sleep(1)
            self.client.connect(self.ip_addr, self.rack, self.slot)
            return bool(self.client.get_connected())
        except Exception as e:
            self._last_connect_error = e
            try:
                self.client.destroy()
            except Exception:
                pass
            try:
                self.client = snap7.client.Client()
                time.sleep(1)
                self.client.connect(self.ip_addr, self.rack, self.slot)
                return bool(self.client.get_connected())
            except Exception as e:
                self._last_connect_error = e
                return False

    def last_connect_error(self) -> str | None:
        """
        Returns the last connection error message.

        :return: str | None: last connection error message.
        """
        return self._last_connect_error

    def start_logging(self, cycle_time_ms: int | float = 20) -> None:
        """
        Starts the background logging in a thread as daemon with a specific cycle time.
        If logging is already active the call is ignored.

        :param cycle_time_ms: int | float: Targeted cycle time in milliseconds. (default: 20ms)
        """
        if self.running:
            return
        self.running = True
        self.thread = threading.Thread(target=self._logging_loop, args=(cycle_time_ms,), daemon=True, name="Snap7DBDync")
        self.thread.start()

    def write_to_plc(self, changes: dict):
        """
        Updates specific variables in the PLC via a Read-Patch-Write cycle.
        Thread-safe method that ensures bit-level accuracy for Booleans and proper byte-swapping for multibyte types.
        The values are not written in the shared memory here; cyclic logging should reflect the changes into shared memory.

        :param changes: dict: Dictionary of changes to be written: { "variable_name": new_value }.
        :return: bool: True if to write was successful false otherwise.
        """
        if not isinstance(changes, dict) or not changes:
            return False
        with self.lock:
            try:
                # 1. Read current state to ensure we only change the targeted bits/bytes
                current_buffer = bytearray(self._read_db())

                for name, value in changes.items():
                    if name not in self.data_struct:
                        continue

                    meta = self.data_struct[name]
                    offset = meta['offset']
                    dtype = meta['type'].lower()

                    # Logic for all types identified in your parser
                    if dtype == 'bool':
                        snap7.util.set_bool(current_buffer, offset, meta['bit'], bool(value))
                    elif dtype == 'int':
                        snap7.util.set_int(current_buffer, offset, int(value))
                    elif dtype == 'real':
                        snap7.util.set_real(current_buffer, offset, float(value))
                    elif dtype == 'word':
                        snap7.util.set_word(current_buffer, offset, int(value))
                    elif dtype == 'byte':
                        current_buffer[offset] = int(value) & 0xFF
                    elif dtype == 'dint' or dtype == 'time':
                        snap7.util.set_dint(current_buffer, offset, int(value))
                    elif dtype == 'dword':
                        snap7.util.set_dword(current_buffer, offset, int(value))

                # 3. Write the patched buffer back to the PLC
                self.client.db_write(self.db_num, 0, current_buffer)
                return True

            except Exception as e:
                print(f"Write error: {e}")
                return False

    def stop_logging(self) -> None:
        """
        Stops the background logging thread with a small timeout.
        """
        self.running = False
        if hasattr(self, 'thread'):
            self.thread.join(timeout=2.0)

    def close_connection(self) -> None:
        """
        Stops the cyclic reading of PLC DB into the shared memory.
        Disconnects the PLC and destroys the client object.
        Closes the shared memory and unlinks the shared memory handle.
        Resets the shared memory back to None.

        This method is suggested to be executed always at the end as cleanup.
        """
        if self.running:
            self.stop_logging()
            try:
                self.client.disconnect()
                self.client.destroy()
            except Exception:
                pass
        try:
            self.shm.close()
        except Exception:
            pass
        try:
            self.shm.unlink()
            self.shm = None
        except FileNotFoundError:
            pass
