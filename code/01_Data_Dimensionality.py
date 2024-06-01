from pandas import read_csv, DataFrame
from matplotlib.pyplot import figure, savefig, show
from dslabs_functions import plot_bar_chart
from pandas import Series, to_numeric, to_datetime
import matplotlib.pyplot as plt

filename = "data/energy_dataset.csv"
file_tag = "energy_dataset"
data: DataFrame = read_csv(filename, na_values=" ", index_col='time', parse_dates=True)
print(data['time'].head())
print(data.shape)

figure()
values: dict[str, int] = {"Number of Records": data.shape[0], "Number of Variables": data.shape[1]}
plot_bar_chart(
    list(values.keys()), list(values.values()), title="Number of Records vs Number of Variables"
)
plt.tight_layout()

#savefig(f"images/{file_tag}_records_variables.png")
#show()

mv: dict[str, int] = {}
for var in data.columns:
    nr: int = data[var].isna().sum()
    if nr > 0:
        mv[var] = nr

figure(figsize=(7,7))
plot_bar_chart(
    list(mv.keys()),
    list(mv.values()),
    title="Number of missing values per variable",
    xlabel="Variables",
    ylabel="Number of missing values",
)
plt.tight_layout()

#(f"images/{file_tag}_mv.png")
#show()

print(data.dtypes)


def get_variable_types(df: DataFrame) -> dict[str, list]:
    variable_types: dict = {"numeric": [], "binary": [], "date": [], "symbolic": []}

    nr_values: Series = df.nunique(axis=0, dropna=True)
    for c in df.columns:
        if 2 == nr_values[c]:
            variable_types["binary"].append(c)
            df[c].astype("bool")
        else:
            try:
                to_numeric(df[c], errors="raise")
                variable_types["numeric"].append(c)
            except ValueError:
                try:
                    df[c] = to_datetime(df[c], errors="raise")
                    variable_types["date"].append(c)
                except ValueError:
                    variable_types["symbolic"].append(c)

    return variable_types

variable_types: dict[str, list] = get_variable_types(data)
print(variable_types)
counts: dict[str, int] = {}
for tp in variable_types.keys():
    counts[tp] = len(variable_types[tp])

#figure()
plot_bar_chart(
    list(counts.keys()), list(counts.values()), title="Number of variables per type"
)
plt.tight_layout()
savefig(f"images/{file_tag}_variable_types.png")
#show()

symbolic: list[str] = variable_types["symbolic"]
data[symbolic] = data[symbolic].apply(lambda x: x.astype("category"))
data.dtypes