import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np


# sns.set_style("whitegrid", { 'grid.color': '.9'})


def plot_heatmap(df, ax=None, **kwargs):
    ax = ax or plt.gca()
    sns.heatmap(df, vmax=np.nanmax(df), vmin=np.nanmean(df), **kwargs)
    sns.set(context="paper", font="monospace")
    ax.set_yticklabels(ax.yaxis.get_majorticklabels(), rotation=0)
    ax.set_xticklabels(ax.xaxis.get_majorticklabels(), rotation=90)


def plot_scatter(df, labeled=True, ax=None):
    """ Simple scatter plot. """
    ax = ax or plt.gca()
    df.plot(kind='scatter', x='x', y='y', ax=ax)

    if labeled:
        for k, v in df.iterrows():
            ax.annotate(k, v)
