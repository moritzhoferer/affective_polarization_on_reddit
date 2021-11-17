#! /usr/bin/python3

import pandas as pd
from datetime import datetime
from scipy.optimize import curve_fit

import matplotlib as mpl
import matplotlib.pyplot as plt

import config

mpl.rcParams['timezone'] = 'US/Eastern'

def linear_func(x, a, b):
    return a * x + b

df = pd.read_csv(config.db_dir + 'aggregated_sentiment_scores_user_user.csv').dropna()
df = df[df['To']>1.3e9]

xmin = 1293840000 
xmax = 1609459200 


ymin = df.groupby('measurement').min()
ymax = df.groupby('measurement').max()

ylimits = {
    'neg': [.085, .125], 'neu': [.76, .82], 'pos': [.085, .125], 'compound': [-.125, .125],

}

ylimits_diff = {
    'neg': [-.015, .01], 'neu': [-.025, .025], 'pos': [-.015, .01], 'compound': [-.1, .1],

}

ylabels = {'pos': 'Positive', 'neu': 'Neutral', 'neg': 'Negative', 'compound': 'Compound'}


for ho_t in range(2, 7):
    for he_t in range(2, 7):
        fig, ax = plt.subplots(nrows=2, ncols=2, figsize=(6,4))
        def plot_data(i: int, j: int, label: str):
            _df = df[(df['measurement']==label)]
            ax[i,j].vlines(
                config.timestamps2dates(config.ELECTION_TIMESTAMPS),
                ylimits[label][0], ylimits[label][1],
                colors='grey', linestyles='dashed',
                )
            for _t in _df['Type'].unique():
                _threshold = he_t if _t=='heterogeneous' else ho_t
                _sub_df = _df[(_df['Type']==_t) & (_df['Threshold']==_threshold)]
                ax[i,j].plot(
                    config.timestamps2dates(_sub_df['To'].values), _sub_df['mean'],
                    '-o', 
                    color=config.color_dict[_t],
                    markerfacecolor='None',
                    label=_t.capitalize(),
                )


            ax[i,j].set_ylabel(ylabels[label])
            ax[i,j].set_xlabel('Time-interval end')
            ax[i,j].set_xticks(config.timestamps2dates(config.ELECTION_TIMESTAMPS-6*3600))
            ax[i,j].set_xlim([datetime.fromtimestamp(xmin), datetime.fromtimestamp(xmax)])
            ax[i,j].set_ylim(ylimits[label])

        plot_data(0, 0, 'pos')
        ax[0,0].legend(ncol=2, fontsize='small',)

        plot_data(0, 1, 'neu')
        plot_data(1, 0, 'neg')
        plot_data(1, 1, 'compound')

        fig.tight_layout(pad=.2)
        fig.savefig(f'./graphics/aggregated_sentiment_scores_user_user_ho_{ho_t}_he_{he_t}.png')
        plt.close(fig=fig)

        fig, ax = plt.subplots(nrows=2, ncols=2, figsize=(6,4))

        def plot_data_diff(i, j, label):
            _df = df[
                (df['measurement']==label) & 
                (((df['Type']=='heterogeneous') & (df['Threshold']==he_t)) | 
                ((df['Type']=='homogeneous') & (df['Threshold']==ho_t)) |
                ((df['Type']=='democrat') & (df['Threshold']==ho_t)) |
                ((df['Type']=='republican') & (df['Threshold']==ho_t)))]
            _df = _df.pivot(index='To', columns='Type', values='mean').dropna()
            ax[i,j].vlines(
                config.timestamps2dates(config.ELECTION_TIMESTAMPS), -1, 1,
                colors='grey', 
                linestyles='dashed',
            )
            for _label in ['homogeneous', 'democrat', 'republican']:
                _diff = _df['heterogeneous'] - _df[_label]
                ax[i,j].plot(
                    config.timestamps2dates(_diff.index), _diff.values,
                    '-o', 
                    color=config.color_dict[_label],
                    markerfacecolor='None',
                    label=_label.capitalize(),
                    )

                popt, _ = curve_fit(linear_func, _diff.index, _diff.values)
                
                ax[i,j].plot(
                    config.timestamps2dates(_diff.index),
                    linear_func(_diff.index, popt[0], popt[1]),
                    '--', 
                    color=config.color_dict[_label],
                )

            ax[i,j].set_ylabel(ylabels[label])
            ax[i,j].set_xlabel('Time-interval end')
            ax[i,j].set_xticks(config.timestamps2dates(config.ELECTION_TIMESTAMPS-6*3600))
            ax[i,j].set_xlim(datetime.fromtimestamp(xmin), datetime.fromtimestamp(xmax))
            ax[i,j].set_ylim(ylimits_diff[label])

        plot_data_diff(0, 0, 'pos')
        ax[0,0].legend(loc='lower right', ncol=2, fontsize='small')
        plot_data_diff(0, 1, 'neu')
        plot_data_diff(1, 0, 'neg')
        plot_data_diff(1, 1, 'compound')

        fig.tight_layout(pad=.2)
        fig.savefig(f'./graphics/aggregated_sentiment_difference_user_user_ho_{ho_t}_he_{he_t}.png')
        plt.close(fig=fig)
