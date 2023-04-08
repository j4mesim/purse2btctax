#!/usr/bin/python3
"""Command line program to convert CSV's from purse.io to bitcoin.tax import
statements"""
import datetime
import os
from contextlib import contextmanager
from pprint import pformat

from pandas import (
    DataFrame,
    read_csv,
    to_datetime,
    set_option,
    reset_option,
    errors as pandas_errors,
)

purse_cols = [
    "Hash",
    "Type",
    "Date",
    "Volume",
    "Currency",
    "Fee",
    "Currency",
    "Amount",
    "Currency",
    "Discount",
    "Rate",
    "Description",
    "Year",
]

bitcoin_tax_cols = [
    "Date",
    "Action",
    "Symbol",
    "Volume",
    "Currency",
    "Account",
    "Total",
    "Price",
    "Fee",
    "FeeCurrency",
]

datetime_fmt_btctax = "%Y-%m-%d %H:%M:%S.%f+00:00"


@contextmanager
def pandas_printscope(max_rows=10000):
    set_option("display.max_rows", max_rows)
    set_option("display.max_columns", None)
    set_option("display.width", 2000)
    set_option("display.float_format", "{:6.9f}".format)
    set_option("display.max_colwidth", None)
    try:
        yield
    finally:
        reset_option("display.max_rows")
        reset_option("display.max_columns")
        reset_option("display.width")
        reset_option("display.float_format")
        reset_option("display.max_colwidth")


def pandas_print(df):
    with pandas_printscope():
        print(df)


def is_purse_csv(filepath):
    """Check if file is purse file."""
    filepath = os.path.expanduser(filepath)
    if not filepath.endswith(".csv"):
        return False, "wrong extension", {"ext": os.path.ext(filepath)}
    csv_cols = open(filepath).readline()
    csv_cols = list(map(str.strip, csv_cols.split(",")))
    return csv_cols != purse_cols, "mismatch in cols", {"csv_cols": csv_cols}


def get_purse_mask_sales(df_purse):
    """Get mask for only purchases on Purse.io exports"""

    sale = df_purse.Volume.values < 0
    isfinite = ~df_purse.Amount.isna()
    mask = sale & isfinite

    # check mask against shop
    is_shop = [t.lower() == "shop" for t in df_purse.Type.values.tolist()]
    if is_shop != mask.tolist():
        raise ValueError(pformat(dict(is_shop=is_shop, mask=mask)))

    return mask


def get_btc_tax_df(df_purse):
    """Convert df from purse to one for bitcoin.tax importing.

    From the bitcoin.tax website:

        Select one or more comma-separated files from your computer. The files
          must either contains the following fields or have these names in the
          header row:

        Date (date and time as YYYY-MM-DD HH:mm:ss Z)
        Action (BUY, SELL or SWAP)
        Symbol (BTC, LTC, DASH, etc)
        Volume (number of coins traded)
        Currency (specify currency such as USD, GBP, EUR or coins, BTC or LTC)
        Account (override the exchange or wallet name, e.g. Coinbase)
        Total (you can use the total Currency amount or price per coin)
        Price (price per coin in Currency or blank for lookup)
        Fee (any additional costs of the trade)
        FeeCurrency (currency of fee if different than Currency)

        For example,
        Date,Action,Account,Symbol,Volume,Price,Currency,Fee
        2020-01-01 13:00:00 -0800,BUY,Coinbase,BTC,1,500,USD,5.50
    """
    date = (
        to_datetime(df_purse.Date, utc=True).dt.strftime(datetime_fmt_btctax).tolist()
    )
    action = ["SELL"] * len(df_purse)
    symbol = df_purse.iloc[:, 4].values.tolist()  # 1st "Currency"
    volume = list(map(abs, df_purse.loc[:, "Volume"].values.tolist()))
    currency = df_purse.iloc[:, 8].values.tolist()
    account = ["Purse.io"] * len(df_purse)
    total = df_purse.Amount.tolist()
    price = [tot / vol for tot, vol in zip(total, volume)]
    fee = df_purse.Fee.values.tolist()
    fee_currency = df_purse.iloc[:, 6].values.tolist()  # 2nd "Currency"

    # Build new df
    data = list(
        zip(
            date,
            action,
            symbol,
            volume,
            currency,
            account,
            total,
            price,
            fee,
            fee_currency,
        )
    )
    return DataFrame(data, columns=bitcoin_tax_cols)


if __name__ == "__main__":

    import argparse

    progname = os.path.basename(__file__)
    parser = argparse.ArgumentParser(progname, add_help=False)
    parser.add_argument(
        "fp",
        type=str,
        help="Filepath of CSV file of all " "transactions downloaded from " "Purse.io",
    )
    year_now = datetime.datetime.now().year
    parser.add_argument(
        "-y",
        "--year",
        type=int,
        default=year_now - 1,
        help=f"Year to prepare a bitcoin.tax import file ("
        f"default is previous year, so {year_now-1}).",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", default=False, help=f"Display sales."
    )
    parser.add_argument(
        "-d",
        "--dry",
        action="store_true",
        default=False,
        help=f"Display sales but dont export, priority over " f"-v/--verbose option.",
    )
    parser.add_argument(
        "-h", "--help", action="help", help="Help message displayed now."
    )

    args = parser.parse_args()
    if not args.fp.lower().endswith(".csv"):
        raise ValueError(f"file must be csv: {args.fp}")
    elif not os.path.exists(args.fp):
        raise FileNotFoundError(f"file does not exist: {args.fp}")

    is_purse_file, msg, d = is_purse_csv(args.fp)
    if not is_purse_file:
        raise ValueError(
            f"csv {os.path.basename(args.fp)} error: {msg} cols "
            + pformat({"purse_cols": purse_cols, **d})
            + f"\n\nIs '{args.fp}' a Purse.io file?"
        )

    # Get purse dataframe
    try:
        df_purse = read_csv(args.fp)
    except pandas_errors.ParserError as e:
        raise ValueError(f"Is '{args.fp}' a csv file (from Purse.io)?") from e

    # Keep only purchases from purse file.
    mask_purse = get_purse_mask_sales(df_purse)
    df_purse_purchases = df_purse.loc[mask_purse, :].copy()

    # Modify format for bitcoin.tax
    df_btc_tax = get_btc_tax_df(df_purse_purchases)

    mapper = lambda date: date.startswith(str(args.year))
    mask_year = df_btc_tax.Date.map(mapper).values

    if not mask_year.any():
        print(
            f"0 Purse.io purchases in the tax year {args.year}. No "
            f"bitcoin.tax import file created."
        )
    else:

        df_btc_tax = df_btc_tax.loc[mask_year, :].reset_index(drop=True).copy()
        fp_btc_tax = args.fp[:-4] + f"_btc_tax_{args.year}.csv"

        # display
        if args.verbose or args.dry:
            pandas_print(df_btc_tax)

        # Save to file.
        if not args.dry:
            df_btc_tax.to_csv(fp_btc_tax, index=False)
            print(
                f"{len(df_btc_tax)} sales on Purse.io in the year {args.year} "
                f"saved to {fp_btc_tax}"
            )
