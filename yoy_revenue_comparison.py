"""
Year-over-Year Revenue Comparison Script

Generates a professional "YoY Revenue Comparison" sheet in Google Sheets with:
- 2024 & 2025 data: Hardcoded from provided values (total revenue)
- 2026 data: Dynamically pulled from staff sheets (auto-updates)
- 2026 Targets: For Chicken and Egg products only (quarterly targets)
- Variance calculations: Year-over-year comparisons + target vs actual for 2026
- Professional styling: Clean colors, formatting, and borders

Supports both local development and CI/CD execution via environment variables.
"""

import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)

import pandas as pd
import gspread
import time
import functools
import hashlib
import json
import os
import sys
from datetime import datetime, timezone, timedelta

# West Africa Time (WAT) is UTC+1
WAT = timezone(timedelta(hours=1))
from gspread.exceptions import APIError
from gspread_formatting import (
    format_cell_range, format_cell_ranges, CellFormat, Color, TextFormat,
    set_column_width, set_frozen, Border, Borders
)

# Default sheet name (not sensitive)
TARGET_SHEET_NAME = "YoY Revenue Comparison"

# Column mapping from 2026 template to dashboard fields
COLUMN_MAPPING_2026 = {
    'quickbooks_invoice_number': 'invoice_no',
    'date': 'date',
    'customer_name': 'customer_name',
    'product_type': 'product_type',
    'revenue_n': 'amount',
    'year': 'year',
    'month': 'month',
}

# Color scheme for professional styling
COLORS = {
    'header': {'red': 0.2, 'green': 0.4, 'blue': 0.6},
    'subheader': {'red': 0.85, 'green': 0.9, 'blue': 0.95},
    'positive': {'red': 0.85, 'green': 0.95, 'blue': 0.85},
    'negative': {'red': 0.98, 'green': 0.85, 'blue': 0.85},
    'total_row': {'red': 0.95, 'green': 0.95, 'blue': 0.95},
    'data': {'red': 1.0, 'green': 1.0, 'blue': 1.0},
    'title': {'red': 0.15, 'green': 0.3, 'blue': 0.5},
}

# Month order for sorting
MONTH_ORDER = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

# Hash file path (relative to script directory)
HASH_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.last_hash')

# Sales Rep Dashboard hash file
SALES_REP_HASH_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.sales_rep_hash')

# Sales Rep Dashboard sheet name
SALES_REP_SHEET_NAME = "Sales Rep Targets vs Actuals"


def load_config():
    """
    Load sheet configuration from environment variable or local fallback.

    Environment variable SHEET_CONFIG should contain JSON with:
    - target_spreadsheet_id, staff_sheets
    - revenue_2024, revenue_2025 (monthly data)
    - targets_2026 (quarterly targets)
    """
    config_json = os.environ.get('SHEET_CONFIG')
    if config_json:
        config = json.loads(config_json)
    else:
        # Local development fallback - load from local file if exists
        local_config_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            'local_config.json'
        )
        if os.path.exists(local_config_path):
            with open(local_config_path, 'r') as f:
                config = json.load(f)
        else:
            raise ValueError(
                "No configuration found. Set SHEET_CONFIG environment variable "
                "or create local_config.json for local development."
            )

    # Calculate quarterly totals from monthly data
    for year in ['revenue_2024', 'revenue_2025']:
        if year in config:
            monthly = config[year]
            config[f'{year}_quarterly'] = {
                'Q1': monthly['Jan'] + monthly['Feb'] + monthly['Mar'],
                'Q2': monthly['Apr'] + monthly['May'] + monthly['Jun'],
                'Q3': monthly['Jul'] + monthly['Aug'] + monthly['Sep'],
                'Q4': monthly['Oct'] + monthly['Nov'] + monthly['Dec']
            }
            config[f'{year}_total'] = sum(monthly.values())

    return config


def get_google_sheets_client():
    """
    Get authenticated Google Sheets client.
    Uses GOOGLE_CREDENTIALS env var in CI, local file for development.
    """
    creds_json = os.environ.get('GOOGLE_CREDENTIALS')
    if creds_json:
        # Running in CI - use env var
        from google.oauth2.service_account import Credentials
        creds_dict = json.loads(creds_json)
        creds = Credentials.from_service_account_info(creds_dict, scopes=[
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ])
        return gspread.authorize(creds)
    else:
        # Local development - use file
        local_creds_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            'pullus-pipeline-40a5302e034d.json'
        )
        return gspread.service_account(filename=local_creds_path)


def compute_data_hash(df):
    """
    Compute MD5 hash of the extracted DataFrame.
    Used for change detection to avoid unnecessary updates.
    """
    if df.empty:
        return "empty"

    # Sort for consistency, then hash
    df_sorted = df.sort_values(by=df.columns.tolist()).reset_index(drop=True)
    data_str = df_sorted.to_json()
    return hashlib.md5(data_str.encode()).hexdigest()


def get_stored_hash():
    """Retrieve stored hash from file."""
    if os.path.exists(HASH_FILE):
        with open(HASH_FILE, 'r') as f:
            return f.read().strip()
    return None


def store_hash(hash_value):
    """Store hash in file."""
    with open(HASH_FILE, 'w') as f:
        f.write(hash_value)


def get_sales_rep_stored_hash():
    """Retrieve stored hash from sales rep hash file."""
    if os.path.exists(SALES_REP_HASH_FILE):
        with open(SALES_REP_HASH_FILE, 'r') as f:
            return f.read().strip()
    return None


def store_sales_rep_hash(hash_value):
    """Store hash in sales rep hash file."""
    with open(SALES_REP_HASH_FILE, 'w') as f:
        f.write(hash_value)


def smart_retry(max_retries=5, initial_delay=2.0):
    """
    Smart retry decorator that only adds delays when API calls actually fail.
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None

            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except (APIError, Exception) as e:
                    last_exception = e

                    is_rate_limit = (
                        isinstance(e, APIError) and
                        (e.response.status_code in [429, 503, 500] or
                         'quota' in str(e).lower() or
                         'rate' in str(e).lower())
                    )

                    if not is_rate_limit or attempt == max_retries - 1:
                        raise e

                    import random
                    delay = initial_delay * (2 ** attempt)
                    jitter = random.uniform(0.5, 1.5)
                    actual_delay = min(delay * jitter, 30.0)
                    print(f"  Rate limit hit, retrying in {actual_delay:.0f}s...")
                    time.sleep(actual_delay)

            raise last_exception
        return wrapper
    return decorator


@smart_retry(max_retries=3, initial_delay=1.0)
def extract_2026_revenue(config, preserve_staff_identity=False):
    """
    Extract 2026 sales data from all staff sheets.
    Returns a DataFrame with date, month, product_type, and amount columns.

    Args:
        config: Configuration dictionary with staff_sheets
        preserve_staff_identity: If True, adds 'staff_name' and 'sheet_id' columns
                                 instead of generic 'source' column

    Logging is sanitized - no sheet IDs or staff names in output.
    """
    gc = get_google_sheets_client()
    all_dataframes = []

    staff_sheets = config.get('staff_sheets', {})
    total_sources = len(staff_sheets)

    for i, (sheet_id, staff_name) in enumerate(staff_sheets.items()):
        source_num = i + 1
        try:
            if i > 0:
                time.sleep(min(1.0 + (i * 0.3), 4.0))

            print(f"  Extracting from source {source_num}/{total_sources}...")
            spreadsheet = gc.open_by_key(sheet_id)
            worksheet = spreadsheet.worksheet("Daily Sales Log")

            time.sleep(1.0)
            all_values = worksheet.get_all_values()

            if len(all_values) <= 3:
                print(f"    Source {source_num}: No data rows")
                continue

            headers = all_values[2]
            data_rows = all_values[3:]

            if not data_rows:
                continue

            # Clean headers
            cleaned_headers = []
            for j, header in enumerate(headers):
                clean_header = str(header).strip() if header else ""
                if not clean_header:
                    clean_header = f"column_{j+1}"
                else:
                    clean_header = clean_header.lower()
                    clean_header = clean_header.replace(' ', '_').replace('(', '').replace(')', '')
                    clean_header = clean_header.replace('.', '').replace('-', '_').replace('/', '_')
                    clean_header = '_'.join(filter(None, clean_header.split('_')))
                cleaned_headers.append(clean_header)

            # Make headers unique
            unique_headers = []
            seen_headers = {}
            for header in cleaned_headers:
                if header in seen_headers:
                    seen_headers[header] += 1
                    unique_header = f"{header}_{seen_headers[header]}"
                else:
                    seen_headers[header] = 0
                    unique_header = header
                unique_headers.append(unique_header)

            # Normalize rows
            max_cols = len(unique_headers)
            normalized_rows = []
            for row in data_rows:
                if len(row) < max_cols:
                    row = row + [''] * (max_cols - len(row))
                elif len(row) > max_cols:
                    row = row[:max_cols]
                normalized_rows.append(row)

            df = pd.DataFrame(normalized_rows, columns=unique_headers)

            # Apply column mapping
            rename_dict = {}
            for old_col in df.columns:
                if old_col in COLUMN_MAPPING_2026:
                    rename_dict[old_col] = COLUMN_MAPPING_2026[old_col]
            df = df.rename(columns=rename_dict)

            # Clean amount column
            if 'amount' in df.columns:
                df['amount'] = df['amount'].astype(str).str.replace(',', '').str.replace('"', '').str.strip()
                df['amount'] = df['amount'].replace(['', 'nan', 'None'], '0')
                df['amount'] = pd.to_numeric(df['amount'], errors='coerce').fillna(0)

            # Clean product_type column
            if 'product_type' in df.columns:
                df['product_type'] = df['product_type'].astype(str).str.strip()

            # Parse month from month column or date column
            if 'month' in df.columns:
                df['month'] = df['month'].astype(str).str.strip().str.title()
                # Handle full month names
                month_mapping = {
                    'January': 'Jan', 'February': 'Feb', 'March': 'Mar',
                    'April': 'Apr', 'May': 'May', 'June': 'Jun',
                    'July': 'Jul', 'August': 'Aug', 'September': 'Sep',
                    'October': 'Oct', 'November': 'Nov', 'December': 'Dec'
                }
                df['month'] = df['month'].replace(month_mapping)
            elif 'date' in df.columns:
                # Parse month from date column
                df['date'] = pd.to_datetime(df['date'], errors='coerce')
                df['month'] = df['date'].dt.strftime('%b')

            # Remove rows with zero amount
            df = df[df['amount'] > 0]

            # Add source identifier
            if preserve_staff_identity:
                df['staff_name'] = staff_name
                df['sheet_id'] = sheet_id
            else:
                df['source'] = f"source_{source_num}"

            if not df.empty:
                all_dataframes.append(df)
                print(f"    Source {source_num}: {len(df)} records")

        except Exception as e:
            # Sanitized error logging - no sensitive details
            error_type = type(e).__name__
            print(f"    Source {source_num}: Error ({error_type})")
            continue

    if all_dataframes:
        final_df = pd.concat(all_dataframes, ignore_index=True, sort=False)
        return final_df

    return pd.DataFrame()


def aggregate_by_month(df):
    """
    Aggregate revenue by month across all products.
    Returns a dictionary with month as key and total revenue as value.
    """
    if df.empty:
        return {month: 0 for month in MONTH_ORDER}

    monthly = df.groupby('month')['amount'].sum().to_dict()

    # Ensure all months are present
    result = {}
    for month in MONTH_ORDER:
        result[month] = monthly.get(month, 0)

    return result


def aggregate_by_quarter(df):
    """
    Aggregate revenue by quarter across all products.
    Returns a dictionary with quarter as key and total revenue as value.
    """
    quarter_mapping = {
        'Jan': 'Q1', 'Feb': 'Q1', 'Mar': 'Q1',
        'Apr': 'Q2', 'May': 'Q2', 'Jun': 'Q2',
        'Jul': 'Q3', 'Aug': 'Q3', 'Sep': 'Q3',
        'Oct': 'Q4', 'Nov': 'Q4', 'Dec': 'Q4'
    }

    if df.empty:
        return {'Q1': 0, 'Q2': 0, 'Q3': 0, 'Q4': 0}

    df_copy = df.copy()
    df_copy['quarter'] = df_copy['month'].map(quarter_mapping)
    quarterly = df_copy.groupby('quarter')['amount'].sum().to_dict()

    result = {}
    for q in ['Q1', 'Q2', 'Q3', 'Q4']:
        result[q] = quarterly.get(q, 0)

    return result


def filter_chicken_egg(df):
    """
    Filter DataFrame for Whole Chicken and Egg products only.
    Returns separate DataFrames for chicken and egg.
    """
    if df.empty:
        return pd.DataFrame(), pd.DataFrame()

    # Filter for Whole Chicken (exact match, case-insensitive)
    chicken_df = df[df['product_type'].str.lower().str.strip() == 'whole chicken'].copy()

    # Filter for Egg/Eggs (case-insensitive)
    egg_df = df[df['product_type'].str.lower().str.strip().isin(['egg', 'eggs'])].copy()

    return chicken_df, egg_df


def aggregate_by_staff_quarter_product(df, rep_config):
    """
    Aggregate revenue by staff, quarter, and product type.

    Args:
        df: DataFrame with staff_name, sheet_id, month, product_type, amount columns
        rep_config: Dictionary mapping sheet_id to rep configuration

    Returns:
        Dictionary: {sheet_id: {quarter: {egg: amount, wc: amount}}}
    """
    if df.empty:
        return {}

    quarter_mapping = {
        'Jan': 'Q1', 'Feb': 'Q1', 'Mar': 'Q1',
        'Apr': 'Q2', 'May': 'Q2', 'Jun': 'Q2',
        'Jul': 'Q3', 'Aug': 'Q3', 'Sep': 'Q3',
        'Oct': 'Q4', 'Nov': 'Q4', 'Dec': 'Q4'
    }

    result = {}

    # Filter only for reps in config
    rep_sheet_ids = set(rep_config.keys())

    for sheet_id in rep_sheet_ids:
        result[sheet_id] = {
            'Q1': {'egg': 0, 'wc': 0},
            'Q2': {'egg': 0, 'wc': 0},
            'Q3': {'egg': 0, 'wc': 0},
            'Q4': {'egg': 0, 'wc': 0}
        }

    # Filter for relevant sheet_ids
    df_filtered = df[df['sheet_id'].isin(rep_sheet_ids)].copy()

    if df_filtered.empty:
        return result

    # Add quarter column
    df_filtered['quarter'] = df_filtered['month'].map(quarter_mapping)

    # Normalize product type
    df_filtered['product_category'] = df_filtered['product_type'].str.lower().str.strip()

    for _, row in df_filtered.iterrows():
        sheet_id = row['sheet_id']
        quarter = row['quarter']
        product = row['product_category']
        amount = row['amount']

        if quarter is None or sheet_id not in result:
            continue

        if product == 'whole chicken':
            result[sheet_id][quarter]['wc'] += amount
        elif product in ['egg', 'eggs']:
            result[sheet_id][quarter]['egg'] += amount

    return result


@smart_retry(max_retries=3, initial_delay=2.0)
def create_sales_rep_dashboard(config, staff_actuals):
    """
    Create or update the Sales Rep Targets vs Actuals dashboard.

    Args:
        config: Configuration dictionary with sales_rep_dashboard section
        staff_actuals: Dictionary from aggregate_by_staff_quarter_product()

    Returns:
        Tuple of (worksheet, formatting_data)
    """
    rep_config = config['sales_rep_dashboard']['reps']
    target_sheet_id = config['sales_rep_dashboard']['target_spreadsheet_id']

    gc = get_google_sheets_client()

    print("  Opening sales rep target spreadsheet...")
    spreadsheet = gc.open_by_key(target_sheet_id)

    # Check if old sheet exists (we'll delete it after creating the new one)
    old_worksheet = None
    try:
        old_worksheet = spreadsheet.worksheet(SALES_REP_SHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        pass

    # Create new sheet with temporary name first
    temp_name = f"{SALES_REP_SHEET_NAME}_temp_{int(time.time())}"
    print("  Creating sheet...")
    worksheet = spreadsheet.add_worksheet(title=temp_name, rows=50, cols=10)
    time.sleep(1.0)

    # Now delete the old sheet (safe because we have the new one)
    if old_worksheet:
        print("  Removing old sheet...")
        spreadsheet.del_worksheet(old_worksheet)
        time.sleep(1.0)

    # Rename the new sheet to the proper name
    worksheet.update_title(SALES_REP_SHEET_NAME)
    time.sleep(1.0)

    # Build data
    all_data = []
    formatting_data = []

    # Row 1: Title
    all_data.append(["SALES REP TARGETS VS ACTUALS DASHBOARD", "", "", "", "", "", "", "", ""])

    # Row 2: Empty
    all_data.append([""])

    # Row 3: Last Updated
    all_data.append([f"Last Updated: {datetime.now(WAT).strftime('%d-%b-%Y %I:%M %p')} WAT"])

    # Row 4: Empty
    all_data.append([""])

    # Row 5: Headers
    all_data.append([
        "Name",
        "Location",
        "Business Contribution %",
        "Quarterly Target Eggs",
        "Quarterly Targets WC",
        "Total Target (Egg+WC)",
        "Total Achieved",
        "Percentage Achieved%",
        "Gap"
    ])

    # Data rows: Each rep x 4 quarters + rep total + grand total
    quarters = ['Q1', 'Q2', 'Q3', 'Q4']

    # Track grand totals (only for quarters with data)
    grand_egg_target = 0
    grand_wc_target = 0
    grand_total_target = 0
    grand_total_achieved = 0

    for sheet_id, rep_info in rep_config.items():
        rep_name = rep_info['name']
        location = rep_info['location']
        contribution = rep_info['business_contribution']
        egg_target = rep_info['quarterly_targets']['egg']
        wc_target = rep_info['quarterly_targets']['wc']
        total_target = egg_target + wc_target

        # Track rep yearly totals (quarterly × 4)
        rep_yearly_egg_target = egg_target * 4
        rep_yearly_wc_target = wc_target * 4
        rep_yearly_total_target = total_target * 4
        rep_total_achieved = 0

        for quarter in quarters:
            # Get actuals
            if sheet_id in staff_actuals:
                egg_actual = staff_actuals[sheet_id][quarter]['egg']
                wc_actual = staff_actuals[sheet_id][quarter]['wc']
            else:
                egg_actual = 0
                wc_actual = 0

            total_actual = egg_actual + wc_actual
            rep_total_achieved += total_actual

            # Format the name with quarter
            name_with_quarter = f"{rep_name} ({quarter})"

            # Only calculate percentage and gap if there's actual data
            if total_actual > 0:
                percentage = (total_actual / total_target) * 100 if total_target > 0 else 0
                gap = total_actual - total_target

                row_data = [
                    name_with_quarter,
                    location,
                    f"{contribution * 100:.0f}%",
                    format_currency(egg_target),
                    format_currency(wc_target),
                    format_currency(total_target),
                    format_currency(total_actual),
                    f"{percentage:.1f}%",
                    format_currency(gap)
                ]

                # Track formatting data (only for rows with actual data)
                formatting_data.append({
                    'row_has_data': True,
                    'is_total_row': False,
                    'percentage': percentage,
                    'gap': gap
                })
            else:
                # No actual data for this quarter - show dashes
                row_data = [
                    name_with_quarter,
                    location,
                    f"{contribution * 100:.0f}%",
                    format_currency(egg_target),
                    format_currency(wc_target),
                    format_currency(total_target),
                    "-",
                    "-",
                    "-"
                ]

                # No conditional formatting needed for empty rows
                formatting_data.append({
                    'row_has_data': False,
                    'is_total_row': False,
                    'percentage': 0,
                    'gap': 0
                })

            all_data.append(row_data)

        # Add rep total row (yearly targets, actual achieved so far)
        if rep_total_achieved > 0:
            rep_percentage = (rep_total_achieved / rep_yearly_total_target) * 100 if rep_yearly_total_target > 0 else 0
            rep_gap = rep_total_achieved - rep_yearly_total_target

            rep_total_row = [
                f"{rep_name} TOTAL",
                "-",
                "-",
                format_currency(rep_yearly_egg_target),
                format_currency(rep_yearly_wc_target),
                format_currency(rep_yearly_total_target),
                format_currency(rep_total_achieved),
                f"{rep_percentage:.1f}%",
                format_currency(rep_gap)
            ]

            formatting_data.append({
                'row_has_data': True,
                'is_total_row': True,
                'percentage': rep_percentage,
                'gap': rep_gap
            })
        else:
            # No data for any quarter - still show yearly targets
            rep_total_row = [
                f"{rep_name} TOTAL",
                "-",
                "-",
                format_currency(rep_yearly_egg_target),
                format_currency(rep_yearly_wc_target),
                format_currency(rep_yearly_total_target),
                "-",
                "-",
                "-"
            ]

            formatting_data.append({
                'row_has_data': False,
                'is_total_row': True,
                'percentage': 0,
                'gap': 0
            })

        all_data.append(rep_total_row)

        # Add to grand totals (yearly)
        grand_egg_target += rep_yearly_egg_target
        grand_wc_target += rep_yearly_wc_target
        grand_total_target += rep_yearly_total_target
        grand_total_achieved += rep_total_achieved

    # Add grand total row
    if grand_total_achieved > 0:
        grand_percentage = (grand_total_achieved / grand_total_target) * 100 if grand_total_target > 0 else 0
        grand_gap = grand_total_achieved - grand_total_target

        grand_total_row = [
            "GRAND TOTAL",
            "-",
            "-",
            format_currency(grand_egg_target),
            format_currency(grand_wc_target),
            format_currency(grand_total_target),
            format_currency(grand_total_achieved),
            f"{grand_percentage:.1f}%",
            format_currency(grand_gap)
        ]

        formatting_data.append({
            'row_has_data': True,
            'is_total_row': True,
            'percentage': grand_percentage,
            'gap': grand_gap
        })
    else:
        grand_total_row = [
            "GRAND TOTAL",
            "-",
            "-",
            format_currency(grand_egg_target),
            format_currency(grand_wc_target),
            format_currency(grand_total_target),
            "-",
            "-",
            "-"
        ]

        formatting_data.append({
            'row_has_data': False,
            'is_total_row': True,
            'percentage': 0,
            'gap': 0
        })

    all_data.append(grand_total_row)

    # Write all data
    print("  Writing data...")
    worksheet.update('A1', all_data, value_input_option='RAW')
    time.sleep(1.0)

    return worksheet, formatting_data


def apply_sales_rep_formatting(worksheet, formatting_data):
    """
    Apply professional formatting to the sales rep dashboard.

    Args:
        worksheet: The gspread worksheet object
        formatting_data: List of dicts with 'percentage' and 'gap' values for each data row
    """
    print("  Applying formatting (batch mode)...")

    # Define formats
    title_format = CellFormat(
        backgroundColor=Color(**COLORS['title']),
        textFormat=TextFormat(bold=True, fontSize=14, foregroundColor=Color(1, 1, 1)),
        horizontalAlignment='CENTER'
    )

    updated_format = CellFormat(
        textFormat=TextFormat(italic=True, fontSize=10),
        horizontalAlignment='LEFT'
    )

    header_format = CellFormat(
        backgroundColor=Color(**COLORS['subheader']),
        textFormat=TextFormat(bold=True, fontSize=10),
        horizontalAlignment='CENTER',
        borders=Borders(
            top=Border('SOLID', Color(0.7, 0.7, 0.7)),
            bottom=Border('SOLID', Color(0.7, 0.7, 0.7)),
            left=Border('SOLID', Color(0.7, 0.7, 0.7)),
            right=Border('SOLID', Color(0.7, 0.7, 0.7))
        )
    )

    data_format = CellFormat(
        backgroundColor=Color(**COLORS['data']),
        horizontalAlignment='RIGHT',
        borders=Borders(
            top=Border('SOLID', Color(0.85, 0.85, 0.85)),
            bottom=Border('SOLID', Color(0.85, 0.85, 0.85)),
            left=Border('SOLID', Color(0.85, 0.85, 0.85)),
            right=Border('SOLID', Color(0.85, 0.85, 0.85))
        )
    )

    name_format = CellFormat(
        backgroundColor=Color(**COLORS['data']),
        horizontalAlignment='LEFT',
        borders=Borders(
            top=Border('SOLID', Color(0.85, 0.85, 0.85)),
            bottom=Border('SOLID', Color(0.85, 0.85, 0.85)),
            left=Border('SOLID', Color(0.85, 0.85, 0.85)),
            right=Border('SOLID', Color(0.85, 0.85, 0.85))
        )
    )

    green_format = CellFormat(
        backgroundColor=Color(**COLORS['positive']),
        textFormat=TextFormat(bold=True)
    )

    red_format = CellFormat(
        backgroundColor=Color(**COLORS['negative']),
        textFormat=TextFormat(bold=True)
    )

    total_row_format = CellFormat(
        backgroundColor=Color(**COLORS['total_row']),
        textFormat=TextFormat(bold=True),
        horizontalAlignment='RIGHT',
        borders=Borders(
            top=Border('SOLID', Color(0.5, 0.5, 0.5)),
            bottom=Border('SOLID', Color(0.5, 0.5, 0.5)),
            left=Border('SOLID', Color(0.5, 0.5, 0.5)),
            right=Border('SOLID', Color(0.5, 0.5, 0.5))
        )
    )

    total_row_name_format = CellFormat(
        backgroundColor=Color(**COLORS['total_row']),
        textFormat=TextFormat(bold=True),
        horizontalAlignment='LEFT',
        borders=Borders(
            top=Border('SOLID', Color(0.5, 0.5, 0.5)),
            bottom=Border('SOLID', Color(0.5, 0.5, 0.5)),
            left=Border('SOLID', Color(0.5, 0.5, 0.5)),
            right=Border('SOLID', Color(0.5, 0.5, 0.5))
        )
    )

    # Calculate the last row (5 header rows + data rows)
    num_data_rows = len(formatting_data)
    last_data_row = 5 + num_data_rows

    # Batch 1: Base formatting
    base_formats = [
        ('A1:I1', title_format),
        ('A3:I3', updated_format),
        ('A5:I5', header_format),
        (f'A6:A{last_data_row}', name_format),
        (f'B6:I{last_data_row}', data_format),
    ]

    format_cell_ranges(worksheet, base_formats)
    time.sleep(2.0)

    # Merge title cells
    worksheet.merge_cells('A1:I1')
    time.sleep(1.0)

    # Batch 2: Total row formatting
    total_row_formats = []
    for i, fmt_data in enumerate(formatting_data):
        row = 6 + i  # Data starts at row 6
        if fmt_data.get('is_total_row', False):
            total_row_formats.append((f'A{row}', total_row_name_format))
            total_row_formats.append((f'B{row}:I{row}', total_row_format))

    if total_row_formats:
        format_cell_ranges(worksheet, total_row_formats)
        time.sleep(2.0)

    # Batch 3: Conditional formatting for percentage and gap columns (only for rows with data)
    conditional_formats = []

    for i, fmt_data in enumerate(formatting_data):
        row = 6 + i  # Data starts at row 6

        # Only apply conditional formatting to rows with actual data
        if fmt_data.get('row_has_data', False):
            # Percentage column (H) - green if >= 100%, red if < 100%
            pct_fmt = green_format if fmt_data['percentage'] >= 100 else red_format
            conditional_formats.append((f'H{row}', pct_fmt))

            # Gap column (I) - green if >= 0, red if < 0
            gap_fmt = green_format if fmt_data['gap'] >= 0 else red_format
            conditional_formats.append((f'I{row}', gap_fmt))

    if conditional_formats:
        format_cell_ranges(worksheet, conditional_formats)
        time.sleep(2.0)

    # Set column widths
    widths = [
        ('A', 150),  # Name
        ('B', 110),  # Location
        ('C', 180),  # Business Contribution
        ('D', 180),  # Quarterly Target Eggs
        ('E', 180),  # Quarterly Targets WC
        ('F', 190),  # Total Target
        ('G', 160),  # Total Achieved
        ('H', 180),  # Percentage Achieved
        ('I', 180),  # Gap
    ]
    for col, width in widths:
        set_column_width(worksheet, col, width)
    time.sleep(1.0)

    # Freeze header rows
    set_frozen(worksheet, rows=5)


def calculate_yoy_variance(current, previous):
    """Calculate year-over-year variance percentage."""
    if previous == 0:
        return 0 if current == 0 else 100
    return ((current - previous) / previous) * 100


def format_currency(value):
    """Format value as Nigerian Naira currency string."""
    if pd.isna(value) or value == 0:
        return "-"
    return f"₦{value:,.2f}"


def format_percentage(value):
    """Format value as percentage string."""
    if pd.isna(value):
        return "-"
    return f"{value:.1f}%"


@smart_retry(max_retries=3, initial_delay=2.0)
def create_comparison_sheet(config, monthly_2026, quarterly_2026, chicken_quarterly_2026, egg_quarterly_2026):
    """
    Create or update the YoY Revenue Comparison sheet with all data sections.
    """
    gc = get_google_sheets_client()
    target_spreadsheet_id = config['target_spreadsheet_id']

    print("  Opening target spreadsheet...")
    spreadsheet = gc.open_by_key(target_spreadsheet_id)

    # Check if old sheet exists (we'll delete it after creating the new one)
    old_worksheet = None
    try:
        old_worksheet = spreadsheet.worksheet(TARGET_SHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        pass

    # Create new sheet with temporary name first
    temp_name = f"{TARGET_SHEET_NAME}_temp_{int(time.time())}"
    print("  Creating sheet...")
    worksheet = spreadsheet.add_worksheet(title=temp_name, rows=50, cols=15)
    time.sleep(1.0)

    # Now delete the old sheet (safe because we have the new one)
    if old_worksheet:
        print("  Removing old sheet...")
        spreadsheet.del_worksheet(old_worksheet)
        time.sleep(1.0)

    # Rename the new sheet to the proper name
    worksheet.update_title(TARGET_SHEET_NAME)
    time.sleep(1.0)

    # Build all data for the sheet
    all_data = []

    # Track variance values for conditional formatting
    monthly_variances = []
    quarterly_variances = []

    # Row 1: Title (will be merged later)
    all_data.append(["PULLUS REVENUE COMPARISON DASHBOARD", "", "", "", "", "", "", "", "", ""])

    # Row 2: Empty
    all_data.append([""])

    # Row 3: Last Updated timestamp
    all_data.append([f"Last Updated: {datetime.now(WAT).strftime('%d-%b-%Y %I:%M %p')} WAT"])

    # Row 4: Empty
    all_data.append([""])

    # Row 5: Section 1 Header - Monthly YoY Comparison
    all_data.append(["SECTION 1: MONTHLY REVENUE COMPARISON (ALL PRODUCTS)", "", "", "", "", ""])

    # Row 6: Column headers for monthly comparison
    all_data.append(["Month", "2026 Actual", "2026 vs 2025", "2025 Actual", "2025 vs 2024", "2024 Actual"])

    # Rows 7-18: Monthly data (Jan-Dec)
    total_2026 = 0
    for month in MONTH_ORDER:
        val_2026 = monthly_2026.get(month, 0)
        val_2025 = config['revenue_2025'].get(month, 0)
        val_2024 = config['revenue_2024'].get(month, 0)

        total_2026 += val_2026

        # Only show variance if we have 2026 data, otherwise show "-"
        if val_2026 > 0:
            var_2026_vs_2025 = calculate_yoy_variance(val_2026, val_2025)
            var_2026_vs_2025_str = format_percentage(var_2026_vs_2025)
        else:
            var_2026_vs_2025 = None
            var_2026_vs_2025_str = "-"

        var_2025_vs_2024 = calculate_yoy_variance(val_2025, val_2024)

        monthly_variances.append({
            'var_2026_vs_2025': var_2026_vs_2025,
            'var_2025_vs_2024': var_2025_vs_2024
        })

        all_data.append([
            month,
            format_currency(val_2026),
            var_2026_vs_2025_str,
            format_currency(val_2025),
            format_percentage(var_2025_vs_2024),
            format_currency(val_2024)
        ])

    # Row 19: Total row
    var_total_2026_vs_2025 = calculate_yoy_variance(total_2026, config['revenue_2025_total'])
    var_total_2025_vs_2024 = calculate_yoy_variance(config['revenue_2025_total'], config['revenue_2024_total'])

    monthly_variances.append({
        'var_2026_vs_2025': var_total_2026_vs_2025 if total_2026 > 0 else None,
        'var_2025_vs_2024': var_total_2025_vs_2024
    })

    all_data.append([
        "TOTAL",
        format_currency(total_2026),
        format_percentage(var_total_2026_vs_2025),
        format_currency(config['revenue_2025_total']),
        format_percentage(var_total_2025_vs_2024),
        format_currency(config['revenue_2024_total'])
    ])

    # Row 20: Empty
    all_data.append([""])

    # Row 21: Section 2 Header - Quarterly YoY Comparison
    all_data.append(["SECTION 2: QUARTERLY REVENUE COMPARISON (ALL PRODUCTS)", "", "", "", "", ""])

    # Row 22: Column headers for quarterly comparison
    all_data.append(["Quarter", "2026 Actual", "2026 vs 2025", "2025 Actual", "2025 vs 2024", "2024 Actual"])

    # Rows 23-26: Quarterly data (Q1-Q4)
    quarterly_total_2026 = 0
    for quarter in ['Q1', 'Q2', 'Q3', 'Q4']:
        val_2026 = quarterly_2026.get(quarter, 0)
        val_2025 = config['revenue_2025_quarterly'].get(quarter, 0)
        val_2024 = config['revenue_2024_quarterly'].get(quarter, 0)

        quarterly_total_2026 += val_2026

        if val_2026 > 0:
            var_2026_vs_2025 = calculate_yoy_variance(val_2026, val_2025)
            var_2026_vs_2025_str = format_percentage(var_2026_vs_2025)
        else:
            var_2026_vs_2025 = None
            var_2026_vs_2025_str = "-"

        var_2025_vs_2024 = calculate_yoy_variance(val_2025, val_2024)

        quarterly_variances.append({
            'var_2026_vs_2025': var_2026_vs_2025,
            'var_2025_vs_2024': var_2025_vs_2024
        })

        all_data.append([
            quarter,
            format_currency(val_2026),
            var_2026_vs_2025_str,
            format_currency(val_2025),
            format_percentage(var_2025_vs_2024),
            format_currency(val_2024)
        ])

    # Row 27: Quarterly total
    var_q_total_2026_vs_2025 = calculate_yoy_variance(quarterly_total_2026, config['revenue_2025_total'])
    var_q_total_2025_vs_2024 = calculate_yoy_variance(config['revenue_2025_total'], config['revenue_2024_total'])

    quarterly_variances.append({
        'var_2026_vs_2025': var_q_total_2026_vs_2025 if quarterly_total_2026 > 0 else None,
        'var_2025_vs_2024': var_q_total_2025_vs_2024
    })

    all_data.append([
        "TOTAL",
        format_currency(quarterly_total_2026),
        format_percentage(var_q_total_2026_vs_2025),
        format_currency(config['revenue_2025_total']),
        format_percentage(var_q_total_2025_vs_2024),
        format_currency(config['revenue_2024_total'])
    ])

    # Row 28: Empty
    all_data.append([""])

    # Row 29: Section 3 Header - 2026 Target vs Actual
    all_data.append(["SECTION 3: 2026 TARGET VS ACTUAL (WHOLE CHICKEN + EGG ONLY)", "", "", "", "", "", "", ""])

    # Row 30: Column headers for target comparison
    all_data.append([
        "Quarter", "Chicken Target", "Chicken Actual", "Chicken %",
        "Egg Target", "Egg Actual", "Egg %",
        "Combined Target", "Combined Actual", "Combined %"
    ])

    # Rows 31-34: Quarterly target vs actual (Q1-Q4)
    target_percentages = []

    chicken_total_actual = 0
    egg_total_actual = 0
    for quarter in ['Q1', 'Q2', 'Q3', 'Q4']:
        chicken_target = config['targets_2026'][quarter]['chicken']
        chicken_actual = chicken_quarterly_2026.get(quarter, 0)
        egg_target = config['targets_2026'][quarter]['egg']
        egg_actual = egg_quarterly_2026.get(quarter, 0)
        combined_target = config['targets_2026'][quarter]['combined']
        combined_actual = chicken_actual + egg_actual

        chicken_total_actual += chicken_actual
        egg_total_actual += egg_actual

        chicken_pct = (chicken_actual / chicken_target * 100) if chicken_target > 0 else 0
        egg_pct = (egg_actual / egg_target * 100) if egg_target > 0 else 0
        combined_pct = (combined_actual / combined_target * 100) if combined_target > 0 else 0

        target_percentages.append({
            'chicken': chicken_pct,
            'egg': egg_pct,
            'combined': combined_pct
        })

        all_data.append([
            quarter,
            format_currency(chicken_target),
            format_currency(chicken_actual),
            f"{chicken_pct:.1f}%",
            format_currency(egg_target),
            format_currency(egg_actual),
            f"{egg_pct:.1f}%",
            format_currency(combined_target),
            format_currency(combined_actual),
            f"{combined_pct:.1f}%"
        ])

    # Row 35: Total row for targets
    chicken_target_total = config['targets_2026']['Total']['chicken']
    egg_target_total = config['targets_2026']['Total']['egg']
    combined_target_total = config['targets_2026']['Total']['combined']
    combined_actual_total = chicken_total_actual + egg_total_actual

    chicken_total_pct = (chicken_total_actual / chicken_target_total * 100) if chicken_target_total > 0 else 0
    egg_total_pct = (egg_total_actual / egg_target_total * 100) if egg_target_total > 0 else 0
    combined_total_pct = (combined_actual_total / combined_target_total * 100) if combined_target_total > 0 else 0

    target_percentages.append({
        'chicken': chicken_total_pct,
        'egg': egg_total_pct,
        'combined': combined_total_pct
    })

    all_data.append([
        "TOTAL",
        format_currency(chicken_target_total),
        format_currency(chicken_total_actual),
        f"{chicken_total_pct:.1f}%",
        format_currency(egg_target_total),
        format_currency(egg_total_actual),
        f"{egg_total_pct:.1f}%",
        format_currency(combined_target_total),
        format_currency(combined_actual_total),
        f"{combined_total_pct:.1f}%"
    ])

    # Write all data to sheet
    print("  Writing data...")
    worksheet.update('A1', all_data, value_input_option='RAW')

    time.sleep(1.0)

    return worksheet, target_percentages, monthly_variances, quarterly_variances


def apply_professional_formatting(worksheet, target_percentages=None, monthly_variances=None, quarterly_variances=None):
    """Apply professional formatting to the comparison sheet using batch updates."""
    print("  Applying formatting (batch mode)...")

    # Define all formats
    title_format = CellFormat(
        backgroundColor=Color(**COLORS['title']),
        textFormat=TextFormat(bold=True, fontSize=14, foregroundColor=Color(1, 1, 1)),
        horizontalAlignment='CENTER'
    )

    updated_format = CellFormat(
        textFormat=TextFormat(italic=True, fontSize=10),
        horizontalAlignment='LEFT'
    )

    section_header_format = CellFormat(
        backgroundColor=Color(**COLORS['header']),
        textFormat=TextFormat(bold=True, fontSize=11, foregroundColor=Color(1, 1, 1)),
        horizontalAlignment='LEFT'
    )

    col_header_format = CellFormat(
        backgroundColor=Color(**COLORS['subheader']),
        textFormat=TextFormat(bold=True, fontSize=10),
        horizontalAlignment='CENTER',
        borders=Borders(
            top=Border('SOLID', Color(0.7, 0.7, 0.7)),
            bottom=Border('SOLID', Color(0.7, 0.7, 0.7)),
            left=Border('SOLID', Color(0.7, 0.7, 0.7)),
            right=Border('SOLID', Color(0.7, 0.7, 0.7))
        )
    )

    data_format = CellFormat(
        backgroundColor=Color(**COLORS['data']),
        horizontalAlignment='RIGHT',
        borders=Borders(
            top=Border('SOLID', Color(0.85, 0.85, 0.85)),
            bottom=Border('SOLID', Color(0.85, 0.85, 0.85)),
            left=Border('SOLID', Color(0.85, 0.85, 0.85)),
            right=Border('SOLID', Color(0.85, 0.85, 0.85))
        )
    )

    total_format = CellFormat(
        backgroundColor=Color(**COLORS['total_row']),
        textFormat=TextFormat(bold=True),
        horizontalAlignment='RIGHT',
        borders=Borders(
            top=Border('SOLID', Color(0.5, 0.5, 0.5)),
            bottom=Border('SOLID', Color(0.5, 0.5, 0.5)),
            left=Border('SOLID', Color(0.5, 0.5, 0.5)),
            right=Border('SOLID', Color(0.5, 0.5, 0.5))
        )
    )

    label_format = CellFormat(horizontalAlignment='LEFT')

    green_format = CellFormat(
        backgroundColor=Color(**COLORS['positive']),
        textFormat=TextFormat(bold=True)
    )

    red_format = CellFormat(
        backgroundColor=Color(**COLORS['negative']),
        textFormat=TextFormat(bold=True)
    )

    # Batch 1: Base formatting (headers, data areas, totals)
    base_formats = [
        ('A1:J1', title_format),
        ('A3:G3', updated_format),
        ('A5:F5', section_header_format),
        ('A21:F21', section_header_format),
        ('A29:J29', section_header_format),
        ('A6:F6', col_header_format),
        ('A22:F22', col_header_format),
        ('A30:J30', col_header_format),
        ('A7:F18', data_format),
        ('A23:F26', data_format),
        ('A31:J34', data_format),
        ('A19:F19', total_format),
        ('A27:F27', total_format),
        ('A35:J35', total_format),
        ('A7:A19', label_format),
        ('A23:A27', label_format),
        ('A31:A35', label_format),
    ]

    format_cell_ranges(worksheet, base_formats)
    time.sleep(2.0)

    # Merge title cells
    worksheet.merge_cells('A1:J1')
    time.sleep(1.0)

    # Batch 2: Conditional formatting for variances
    conditional_formats = []

    # Monthly variances (Rows 7-19)
    if monthly_variances:
        for i, var_data in enumerate(monthly_variances):
            row = 7 + i
            if var_data['var_2026_vs_2025'] is not None:
                fmt = green_format if var_data['var_2026_vs_2025'] >= 0 else red_format
                conditional_formats.append((f'C{row}', fmt))
            fmt = green_format if var_data['var_2025_vs_2024'] >= 0 else red_format
            conditional_formats.append((f'E{row}', fmt))

    # Quarterly variances (Rows 23-27)
    if quarterly_variances:
        for i, var_data in enumerate(quarterly_variances):
            row = 23 + i
            if var_data['var_2026_vs_2025'] is not None:
                fmt = green_format if var_data['var_2026_vs_2025'] >= 0 else red_format
                conditional_formats.append((f'C{row}', fmt))
            fmt = green_format if var_data['var_2025_vs_2024'] >= 0 else red_format
            conditional_formats.append((f'E{row}', fmt))

    # Target percentages (Rows 31-35)
    if target_percentages:
        for i, pct_data in enumerate(target_percentages):
            row = 31 + i
            chicken_fmt = green_format if pct_data['chicken'] >= 100 else red_format
            conditional_formats.append((f'C{row}:D{row}', chicken_fmt))
            egg_fmt = green_format if pct_data['egg'] >= 100 else red_format
            conditional_formats.append((f'F{row}:G{row}', egg_fmt))
            combined_fmt = green_format if pct_data['combined'] >= 100 else red_format
            conditional_formats.append((f'I{row}:J{row}', combined_fmt))

    if conditional_formats:
        format_cell_ranges(worksheet, conditional_formats)
        time.sleep(2.0)

    # Set column widths (these must be done individually)
    widths = [('A', 100), ('B', 140), ('C', 100), ('D', 140), ('E', 100),
              ('F', 140), ('G', 100), ('H', 140), ('I', 140), ('J', 100)]
    for col, width in widths:
        set_column_width(worksheet, col, width)
    time.sleep(1.0)

    # Freeze header rows
    set_frozen(worksheet, rows=1)


def main():
    """Main function to generate dashboards."""
    print("=" * 50)
    print("Dashboard Sync")
    print("=" * 50)

    # Load configuration
    try:
        config = load_config()
        source_count = len(config.get('staff_sheets', {}))
        print(f"Configuration loaded ({source_count} sources)")
    except Exception as e:
        print(f"Configuration error: {type(e).__name__}")
        sys.exit(1)

    # Step 1: Extract 2026 data from staff sheets (with staff identity for both dashboards)
    print("\nStep 1: Extracting 2026 revenue data...")
    df_2026 = extract_2026_revenue(config, preserve_staff_identity=True)

    if df_2026.empty:
        print("  Warning: No data extracted")
        current_hash = "empty"
    else:
        record_count = len(df_2026)
        print(f"  Total records: {record_count}")
        current_hash = compute_data_hash(df_2026)

    # ============================================================
    # YOY REVENUE COMPARISON DASHBOARD
    # ============================================================
    print("\n" + "=" * 50)
    print("YoY Revenue Comparison Sync")
    print("=" * 50)

    stored_hash = get_stored_hash()
    if stored_hash == current_hash:
        print("\nNo changes detected - skipping YoY dashboard update")
    else:
        if df_2026.empty:
            monthly_2026 = {month: 0 for month in MONTH_ORDER}
            quarterly_2026 = {'Q1': 0, 'Q2': 0, 'Q3': 0, 'Q4': 0}
            chicken_quarterly_2026 = {'Q1': 0, 'Q2': 0, 'Q3': 0, 'Q4': 0}
            egg_quarterly_2026 = {'Q1': 0, 'Q2': 0, 'Q3': 0, 'Q4': 0}
        else:
            print("\nChanges detected - proceeding with YoY dashboard update...")

            # Aggregate data
            monthly_2026 = aggregate_by_month(df_2026)
            quarterly_2026 = aggregate_by_quarter(df_2026)

            chicken_df, egg_df = filter_chicken_egg(df_2026)
            print(f"  Chicken records: {len(chicken_df)}")
            print(f"  Egg records: {len(egg_df)}")

            chicken_quarterly_2026 = aggregate_by_quarter(chicken_df)
            egg_quarterly_2026 = aggregate_by_quarter(egg_df)

        # Create the comparison sheet
        print("\nStep 2: Updating YoY dashboard...")
        worksheet, target_percentages, monthly_variances, quarterly_variances = create_comparison_sheet(
            config, monthly_2026, quarterly_2026,
            chicken_quarterly_2026, egg_quarterly_2026
        )

        # Apply professional formatting
        print("\nStep 3: Applying formatting...")
        apply_professional_formatting(worksheet, target_percentages, monthly_variances, quarterly_variances)

        # Store the new hash
        store_hash(current_hash)
        print("\nYoY hash updated")

        print("\n" + "=" * 50)
        print("YoY Dashboard updated successfully")
        print("=" * 50)

    # ============================================================
    # SALES REP TARGETS VS ACTUALS DASHBOARD (separate output)
    # ============================================================
    if 'sales_rep_dashboard' in config:
        print("\n" + "=" * 50)
        print("Sales Rep Targets vs Actuals Sync")
        print("=" * 50)

        rep_config = config['sales_rep_dashboard'].get('reps', {})
        if not rep_config:
            print("  No sales rep configuration found - skipping")
        else:
            # Check if sales rep data has changed (same source data, separate hash)
            stored_sales_rep_hash = get_sales_rep_stored_hash()
            if stored_sales_rep_hash == current_hash:
                print("\nNo changes detected - skipping sales rep dashboard update")
            elif df_2026.empty:
                print("  Warning: No data for sales rep dashboard")
            else:
                print("\nChanges detected - proceeding with sales rep dashboard update...")

                # Aggregate by staff, quarter, and product
                staff_actuals = aggregate_by_staff_quarter_product(df_2026, rep_config)

                # Create the sales rep dashboard
                print("\nStep 2: Updating sales rep dashboard...")
                worksheet, formatting_data = create_sales_rep_dashboard(config, staff_actuals)

                # Apply formatting
                print("\nStep 3: Applying formatting...")
                apply_sales_rep_formatting(worksheet, formatting_data)

                # Store the new hash
                store_sales_rep_hash(current_hash)
                print("\nSales rep hash updated")

                print("\n" + "=" * 50)
                print("Sales Rep Dashboard updated successfully")
                print("=" * 50)

    print("\n" + "=" * 50)
    print("All dashboard sync complete")
    print("=" * 50)


if __name__ == "__main__":
    main()
