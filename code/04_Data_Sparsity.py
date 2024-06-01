from numpy import ndarray
import pandas as pd
from pandas import read_csv, DataFrame, to_datetime
from matplotlib.figure import Figure
from matplotlib.pyplot import figure, subplots, savefig, show
from dslabs_functions import HEIGHT, plot_multi_scatters_chart
import matplotlib.pyplot as plt


file_tag = "energy_dataset"
filename = "data/energy_dataset.csv"
data = read_csv(filename, na_values=" ", index_col="time", parse_dates=['time'])

vars: list = data.columns.to_list()
if [] != vars:
    n: int = len(vars) - 1
    fig: Figure
    axs: ndarray
    fig, axs = subplots(n, n, figsize=(n * HEIGHT, n * HEIGHT), squeeze=False)
    plt.suptitle("Sparsity Study", fontsize=18)
    for i in range(len(vars)):
        var1: str = vars[i]
        for j in range(i + 1, len(vars)):
            var2: str = vars[j]
            plot_multi_scatters_chart(data, var1, var2, ax=axs[i, j - 1])
    plt.tight_layout()
    savefig(f"images/{file_tag}_sparsity_study.png")
    show()
else:
    print("Sparsity class: there are no variables.")

from seaborn import heatmap
from dslabs_functions import get_variable_types

variables_types: dict[str, list] = get_variable_types(data)
numeric: list[str] = variables_types["numeric"]
data[numeric] = data[numeric].apply(pd.to_numeric, errors='coerce')
# Check for constant columns
constant_columns = [col for col in numeric if data[col].nunique() == 1]
print("Constant columns:", constant_columns)

# Remove constant columns if any
data = data.drop(columns=constant_columns)
numeric = [col for col in numeric if col not in constant_columns]
corr_mtx: DataFrame = data[numeric].corr().abs()
print(corr_mtx)
figure(figsize=(10, 10))
plt.suptitle("Correlation Analysis", fontsize=18)
heatmap(
    abs(corr_mtx),
    xticklabels=numeric,
    yticklabels=numeric,
    annot=False,
    cmap="Blues",
    vmin=0,
    vmax=1,
)
plt.tight_layout()
savefig(f"images/{file_tag}_correlation_analysis.png")
show()
