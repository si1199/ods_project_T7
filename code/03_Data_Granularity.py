import pandas as pd
from pandas import DataFrame, to_datetime, read_csv, Series
from numpy import ndarray
from matplotlib.figure import Figure
from matplotlib.pyplot import subplots, savefig, show
from dslabs_functions import get_variable_types, plot_bar_chart, HEIGHT
import matplotlib.pyplot as plt

def derive_date_variables(df: DataFrame, date_vars: list[str]) -> DataFrame:
    for date in date_vars:
        df[date + "_year"] = df[date].dt.year
        df[date + "_quarter"] = df[date].dt.quarter
        df[date + "_month"] = df[date].dt.month
        df[date + "_day"] = df[date].dt.day
        df[date + "_hour"] = df[date].dt.hour  # Adding hour extraction
    return df

def analyse_date_granularity(data: DataFrame, var: str, levels: list[str]) -> ndarray:
    unique_years = data[var + "_year"].unique()
    rows = len(unique_years)
    cols = len(levels)

    fig: Figure
    axs: ndarray
    fig, axs = subplots(rows, cols, figsize=(cols * HEIGHT, rows * HEIGHT), squeeze=False)
    fig.suptitle(f"Granularity study for {var}", fontsize=18)

    for r, year in enumerate(sorted(unique_years)):
        year_data = data[data[var + "_year"] == year]
        for c, level in enumerate(levels):
            counts: Series[int] = year_data[var + "_" + level].value_counts().sort_index()
            plot_bar_chart(
                counts.index.to_list(),
                counts.to_list(),
                ax=axs[r, c],
                title=f"{level} ({year})",
                xlabel=level,
                ylabel="nr records",
                percentage=False,
            )
            axs[r, c].tick_params(axis='x', labelsize=8)  # Manage x-tick size
            axs[r, c].tick_params(axis='y', labelsize=10)  # Manage y-tick size
            axs[r, c].set_xlabel(level, fontsize=12)  # Manage x-axis label size
            axs[r, c].set_ylabel("nr records", fontsize=12)  # Manage y-axis label size

    plt.tight_layout(rect=[0, 0, 1, 0.96])  # Adjust layout to make room for the title
    return axs

file_tag = "energy_dataset"
filename = "data/energy_dataset.csv"
data = read_csv(filename, na_values=" ", index_col=None, parse_dates=['time'])
data["time"] = to_datetime(data["time"], format='%Y-%m-%d %H:%M:%S%z', utc=True)
print(data.dtypes[0])

variables_types: dict[str, list] = get_variable_types(data)
data_ext: DataFrame = derive_date_variables(data, variables_types["date"])

# Include hour in the analysis
for v_date in variables_types["date"]:
    axs = analyse_date_granularity(data_ext, v_date, ["year", "quarter", "month", "day", "hour"])
    savefig(f"images/{file_tag}_granularity_{v_date}.png")
    show()
