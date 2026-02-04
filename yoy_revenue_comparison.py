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
def extract_2026_revenue(config):
    """
    Extract 2026 sales data from all staff sheets.
    Returns a DataFrame with date, month, product_type, and amount columns.

    Logging is sanitized - no sheet IDs or staff names in output.
    """
    gc = get_google_sheets_client()
    all_dataframes = []

    staff_sheets = config.get('staff_sheets', {})
    total_sources = len(staff_sheets)

    for i, (sheet_id, _) in enumerate(staff_sheets.items()):
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

            # Add source identifier (anonymized)
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
    if value >= 0:
        return f"{value:.1f}%"
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

    # Check if sheet exists, delete and recreate it to clear all data AND formatting
    try:
        worksheet = spreadsheet.worksheet(TARGET_SHEET_NAME)
        print("  Clearing existing sheet...")
        spreadsheet.del_worksheet(worksheet)
        time.sleep(1.0)
    except gspread.exceptions.WorksheetNotFound:
        pass

    print("  Creating sheet...")
    worksheet = spreadsheet.add_worksheet(title=TARGET_SHEET_NAME, rows=50, cols=15)

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
    """Main function to generate the YoY Revenue Comparison sheet."""
    print("=" * 50)
    print("YoY Revenue Comparison Sync")
    print("=" * 50)

    # Load configuration
    try:
        config = load_config()
        source_count = len(config.get('staff_sheets', {}))
        print(f"Configuration loaded ({source_count} sources)")
    except Exception as e:
        print(f"Configuration error: {type(e).__name__}")
        sys.exit(1)

    # Step 1: Extract 2026 data from staff sheets
    print("\nStep 1: Extracting 2026 revenue data...")
    df_2026 = extract_2026_revenue(config)

    if df_2026.empty:
        print("  Warning: No data extracted")
        monthly_2026 = {month: 0 for month in MONTH_ORDER}
        quarterly_2026 = {'Q1': 0, 'Q2': 0, 'Q3': 0, 'Q4': 0}
        chicken_quarterly_2026 = {'Q1': 0, 'Q2': 0, 'Q3': 0, 'Q4': 0}
        egg_quarterly_2026 = {'Q1': 0, 'Q2': 0, 'Q3': 0, 'Q4': 0}
        current_hash = "empty"
    else:
        record_count = len(df_2026)
        print(f"  Total records: {record_count}")

        # Compute hash of extracted data
        current_hash = compute_data_hash(df_2026)

        # Step 2: Check if data has changed
        stored_hash = get_stored_hash()
        if stored_hash == current_hash:
            print("\nNo changes detected - skipping update")
            print("=" * 50)
            return

        print("\nChanges detected - proceeding with update...")

        # Aggregate data
        monthly_2026 = aggregate_by_month(df_2026)
        quarterly_2026 = aggregate_by_quarter(df_2026)

        chicken_df, egg_df = filter_chicken_egg(df_2026)
        print(f"  Chicken records: {len(chicken_df)}")
        print(f"  Egg records: {len(egg_df)}")

        chicken_quarterly_2026 = aggregate_by_quarter(chicken_df)
        egg_quarterly_2026 = aggregate_by_quarter(egg_df)

    # Step 3: Create the comparison sheet
    print("\nStep 2: Updating dashboard...")
    worksheet, target_percentages, monthly_variances, quarterly_variances = create_comparison_sheet(
        config, monthly_2026, quarterly_2026,
        chicken_quarterly_2026, egg_quarterly_2026
    )

    # Step 4: Apply professional formatting
    print("\nStep 3: Applying formatting...")
    apply_professional_formatting(worksheet, target_percentages, monthly_variances, quarterly_variances)

    # Step 5: Store the new hash
    store_hash(current_hash)
    print("\nHash updated")

    print("\n" + "=" * 50)
    print("Dashboard updated successfully")
    print("=" * 50)


if __name__ == "__main__":
    main()
