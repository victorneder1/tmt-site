r"""
Refresh Visible Alpha data in Excel, wait for data to load, save and upload.

Usage:
    python update_excel.py
"""

import os
import sys
import time
import subprocess
import logging
import glob

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
MAX_WAIT = 300
POLL_INTERVAL = 5

VA_ADDIN_DIR = os.path.join(
    os.environ["LOCALAPPDATA"], "Visible Alpha", "Visible Alpha Excel Add-in"
)

FILES = [
    {
        "path": os.path.join(SCRIPT_DIR, "Screening_VisibleAlpha_Software_site.xlsx"),
        "sheet": "Comps_GAAP",
        "check_cells": [(5, 4), (125, 4)],
    },
    {
        "path": os.path.join(SCRIPT_DIR, "Screening_VisibleAlpha_ITServices_site.xlsx"),
        "sheet": "Comps_ITServices",
        "check_cells": [(5, 4), (24, 4)],
    },
]

BAD_PREFIXES = ("#Loading", "#NAME")

log_path = os.path.join(SCRIPT_DIR, "update_log.txt")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(message)s",
    handlers=[
        logging.FileHandler(log_path, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)


def kill_other_instances():
    import win32com.client
    my_pid = os.getpid()
    wmi = win32com.client.GetObject(r"winmgmts:\\.\root\cimv2")
    query = (
        "SELECT ProcessId, CommandLine FROM Win32_Process "
        "WHERE Name = 'python.exe' OR Name = 'pythonw.exe'"
    )
    for proc in wmi.ExecQuery(query):
        try:
            cmd = proc.CommandLine or ""
            if "update_excel.py" in cmd and proc.ProcessId != my_pid:
                log.info(f"Killing previous instance PID {proc.ProcessId}")
                proc.Terminate()
        except Exception:
            pass


def kill_excel():
    """Kill all running Excel instances to avoid RPC_E_CALL_REJECTED."""
    try:
        result = subprocess.run(
            ["taskkill", "/f", "/im", "EXCEL.EXE"],
            capture_output=True, text=True, timeout=10,
        )
        if result.returncode == 0:
            log.info("Killed existing Excel instances.")
            time.sleep(3)
    except Exception:
        pass


def wait_for_excel(xl, timeout=30):
    """Wait until Excel is ready to accept COM calls."""
    for i in range(timeout):
        try:
            xl._oleobj_.GetIDsOfNames('Visible')
            return True
        except Exception:
            time.sleep(1)
    return False


def _com_retry(func, *args, retries=5, delay=2):
    """Retry a COM call if Excel is busy (RPC_E_CALL_REJECTED)."""
    for attempt in range(retries):
        try:
            return func(*args)
        except Exception as e:
            if "-2147418111" in str(e) and attempt < retries - 1:
                time.sleep(delay)
            else:
                raise


def _get_cell(ws, row, col):
    """Get a cell with retry for busy Excel."""
    return _com_retry(ws.Cells, row, col)


def cell_is_bad(cell):
    try:
        text = str(cell.Text or "")
        for prefix in BAD_PREFIXES:
            if prefix in text:
                return True
    except Exception:
        pass
    return False


def get_cell_status(cell):
    try:
        return str(cell.Text or "(empty)")
    except Exception:
        return "(error)"


def sheet_has_bad_cells(ws):
    try:
        used = ws.UsedRange
        for row in used.Rows:
            for cell in row.Cells:
                if cell_is_bad(cell):
                    return True
    except Exception:
        pass
    return False


def register_va_xll(xl):
    """Try to register the Visible Alpha XLL to make VADetail() available."""
    log.info("Searching for Visible Alpha XLL/DLL files...")

    # Look for XLL files in the VA add-in directory
    xll_patterns = [
        os.path.join(VA_ADDIN_DIR, "*.xll"),
        os.path.join(VA_ADDIN_DIR, "**", "*.xll"),
    ]

    for pattern in xll_patterns:
        for xll_path in glob.glob(pattern, recursive=True):
            log.info(f"  Found XLL: {xll_path}")
            try:
                result = xl.RegisterXLL(xll_path)
                log.info(f"  RegisterXLL result: {result}")
            except Exception as e:
                log.warning(f"  RegisterXLL failed: {e}")

    # Also try the DLL directly
    dll_path = os.path.join(VA_ADDIN_DIR, "adxloader64.VAExcelPlugin.dll")
    if os.path.exists(dll_path):
        log.info(f"  Found DLL: {dll_path}")
        try:
            result = xl.RegisterXLL(dll_path)
            log.info(f"  RegisterXLL(dll) result: {result}")
        except Exception as e:
            log.warning(f"  RegisterXLL(dll) failed: {e}")


def reload_addin(xl):
    """Reconnect Visible Alpha COM add-in."""
    log.info("Reconnecting COM add-in...")
    try:
        for addin in xl.COMAddIns:
            desc = (addin.Description or "").lower()
            progid = (addin.ProgId or "").lower()
            if "visible" in desc or "visible" in progid:
                log.info(f"  Found: {addin.Description} (Connected: {addin.Connect})")
                addin.Connect = False
                time.sleep(2)
                addin.Connect = True
                log.info("  Reconnected.")
                time.sleep(5)
                return True
    except Exception as e:
        log.warning(f"  Error: {e}")
    return False


def trigger_va_refresh_sendkeys(xl):
    """Use SendKeys to click the Visible Alpha Refresh button in the ribbon."""
    import win32gui

    log.info("  Using SendKeys to trigger Visible Alpha refresh...")

    # Bring Excel to foreground
    try:
        hwnd = xl.Hwnd
        win32gui.SetForegroundWindow(hwnd)
        time.sleep(1)
    except Exception as e:
        log.warning(f"  Could not bring Excel to foreground: {e}")

    # Use Alt key sequence to navigate ribbon
    # First try: Press Alt to activate ribbon, then navigate
    try:
        # Send Ctrl+Alt+F5 (common shortcut for refresh all connections)
        xl.SendKeys("^%{F5}", True)
        time.sleep(2)
    except Exception:
        pass

    # Also try F9 to force recalculation
    try:
        xl.SendKeys("{F9}", True)
        time.sleep(2)
    except Exception:
        pass

    # Try Ctrl+Shift+F9 (force full recalculation rebuilding dependencies)
    try:
        xl.SendKeys("^+{F9}", True)
        time.sleep(2)
    except Exception:
        pass


def process_file(xl, file_info, attempt=1):
    """Open workbook, refresh, wait for loading, save."""
    path = file_info["path"]
    sheet_name = file_info["sheet"]
    check_cells = file_info["check_cells"]

    if not os.path.exists(path):
        log.error(f"File not found: {path}")
        return False

    log.info(f"Opening: {os.path.basename(path)} (attempt {attempt})")

    try:
        wb = xl.Workbooks.Open(path)
    except Exception as e:
        log.error(f"  Could not open: {e}")
        return False

    try:
        import win32com.client
        ws = win32com.client.gencache.EnsureDispatch(wb.Sheets(sheet_name))
    except Exception as e:
        log.error(f"  Sheet '{sheet_name}' not found: {e}")
        wb.Close(False)
        return False

    # Log initial state
    for row, col in check_cells:
        log.info(f"  Cell({row},{col}) = {get_cell_status(_get_cell(ws, row, col))}")

    # Check if we have #NAME errors
    has_name = any("#NAME" in get_cell_status(_get_cell(ws, r, c)) for r, c in check_cells)

    if has_name:
        log.info("  #NAME errors detected - triggering add-in refresh...")
        # Try registering XLL again
        register_va_xll(xl)
        time.sleep(3)

        # Reconnect add-in
        reload_addin(xl)

        # Force full recalculation (rebuilds function dependencies)
        log.info("  Forcing full recalculation...")
        try:
            xl.CalculateFullRebuild()
        except Exception:
            pass
        time.sleep(5)

        # Check again
        has_name = any("#NAME" in get_cell_status(_get_cell(ws, r, c)) for r, c in check_cells)
        if has_name:
            log.info("  Still #NAME after CalculateFullRebuild. Trying SendKeys...")
            trigger_va_refresh_sendkeys(xl)
            time.sleep(10)

            # Force recalc again
            try:
                xl.CalculateFullRebuild()
            except Exception:
                pass
            time.sleep(5)

        # Log status after attempts
        for row, col in check_cells:
            log.info(f"  Cell({row},{col}) after fix attempts = {get_cell_status(_get_cell(ws, row, col))}")

    # Standard refresh
    log.info("  Refreshing data...")
    try:
        wb.RefreshAll()
    except Exception:
        pass
    time.sleep(3)
    try:
        xl.CalculateUntilAsyncQueriesDone()
    except Exception:
        pass

    # Poll until data loads
    log.info("  Waiting for data to load...")
    elapsed = 0
    while elapsed < MAX_WAIT:
        time.sleep(POLL_INTERVAL)
        elapsed += POLL_INTERVAL

        all_good = True
        for row, col in check_cells:
            if cell_is_bad(_get_cell(ws, row, col)):
                all_good = False
                if elapsed % 15 == 0:
                    log.info(f"  [{elapsed}s] Cell({row},{col}): {get_cell_status(_get_cell(ws, row, col))}")
                break

        if all_good:
            log.info(f"  Check cells OK after {elapsed}s. Scanning sheet...")
            if not sheet_has_bad_cells(ws):
                for row, col in check_cells:
                    log.info(f"  Cell({row},{col}) final: {get_cell_status(_get_cell(ws, row, col))}")
                log.info("  Saving...")
                wb.Save()
                wb.Close(False)
                return True
            else:
                log.info("  Some cells still bad, waiting...")

        if elapsed % 60 == 0:
            log.info(f"  Retrying refresh after {elapsed}s...")
            try:
                wb.RefreshAll()
                time.sleep(2)
                xl.CalculateUntilAsyncQueriesDone()
            except Exception:
                pass

    # Timeout - if #NAME persists and this is first attempt, retry with fresh Excel
    log.error(f"  TIMEOUT after {MAX_WAIT}s.")
    for row, col in check_cells:
        log.error(f"  Cell({row},{col}) = {get_cell_status(_get_cell(ws, row, col))}")
    wb.Close(False)
    return False


def main():
    import win32com.client
    import pythoncom

    kill_other_instances()
    kill_excel()

    log.info("=" * 50)
    log.info("Starting Excel refresh")

    pythoncom.CoInitialize()

    try:
        win32com.client.gencache.Rebuild()
        xl = win32com.client.gencache.EnsureDispatch("Excel.Application")
    except Exception as e:
        log.error(f"Could not start Excel: {e}")
        sys.exit(1)

    # Set properties with retry (Excel may reject calls while initializing)
    for prop, val in [("Visible", True), ("DisplayAlerts", False),
                      ("AskToUpdateLinks", False), ("AlertBeforeOverwriting", False)]:
        for attempt in range(5):
            try:
                setattr(xl, prop, val)
                break
            except Exception:
                time.sleep(2)

    log.info("Excel started. Waiting for initialization...")
    time.sleep(10)

    # Register XLL and reconnect add-in before opening files
    register_va_xll(xl)
    reload_addin(xl)

    results = []
    for file_info in FILES:
        ok = process_file(xl, file_info)
        results.append(ok)

    try:
        xl.Visible = False
    except Exception:
        pass
    xl.Quit()

    if any(results):
        log.info("Uploading to server...")
        try:
            subprocess.run(
                [sys.executable, os.path.join(SCRIPT_DIR, "upload_to_server.py")],
                timeout=60,
            )
            log.info("Upload complete.")
        except Exception as e:
            log.error(f"Upload failed: {e}")
    else:
        log.error("No files refreshed. Skipping upload.")

    log.info("Finished.")


if __name__ == "__main__":
    main()
