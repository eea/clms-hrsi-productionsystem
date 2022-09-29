#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from si_common.common_functions import *
from product_request_and_download.parse_cosims_products_db import CosimsProductParser
import matplotlib.pyplot as plt
from matplotlib import cm
import traceback
        
def plot_timeliness(output_figure, start_date=None, end_date=None, db_adress=None):
    
    if db_adress is not None:
        os.environ['COSIMS_DB_HTTP_API_BASE_URL'] = db_adress
    
    if start_date is None and end_date is None:
        dt_now = datetime.utcnow()
        start_date = datetime(dt_now.year, dt_now.month, dt_now.day) - timedelta(1)
        end_date = datetime(dt_now.year, dt_now.month, dt_now.day)
    elif start_date is None or end_date is None:
        raise Exception('start_date end end_date must either both be None or both filled')
    
    timeliness_vs_copernicus = []
    timeliness_vs_wekeo = []
    njobs = 0
    njobs_success = 0
    jobs = CosimsProductParser().search_date(start_date, end_date, date_field='l1c_esa_publication_date', add_wekeo_info=True)
    for job in jobs:
        if job['processing_type'] != 'standard' or job['maja_mode'] != 'nominal':
            continue
        njobs += 1
        if job['fsc_path'] is None:
            continue
        njobs_success += 1
        timeliness_vs_copernicus.append((job['fsc_dias_publication_date']-job['l1c_esa_publication_date']).total_seconds()/3600.)
        timeliness_vs_wekeo.append((job['fsc_completion_date']-job['l1c_dias_publication_date']).total_seconds()/3600.)
        if (job['fsc_dias_publication_date']-job['fsc_completion_date']) > timedelta(0,2*3600):
            print('%s / %s:\n  T0: %s\n  T1: %s\n  T2: %s\n  T3: %s\n'%(job['l1c_id'], os.path.basename(job['fsc_path']), job['l1c_esa_publication_date'], job['l1c_dias_publication_date'], job['fsc_completion_date'], job['fsc_dias_publication_date']))
    timeliness_vs_copernicus = np.array(timeliness_vs_copernicus)
    timeliness_vs_wekeo = np.array(timeliness_vs_wekeo)
    
    fig = plt.figure(figsize=(14, 8))
    try:
        ax = fig.add_subplot(1,1,1)
        ax.scatter(timeliness_vs_wekeo, timeliness_vs_copernicus, \
            s=20, marker='.', color='black', label='%d/%d non-cloudy products'%(njobs_success, njobs))
        
        xlims = list(ax.get_xlim())
        xlims[0] = 0.
        if xlims[1] < 3.:
            xlims[1] = 3.
        ax.set_xlim(xlims)
        
        ylims = list(ax.get_ylim())
        ylims[0] = 0.
        if ylims[1] < 3.:
            ylims[1] = 3.
        ax.set_ylim(ylims)
        
        ax.fill_between(np.linspace(xlims[0], xlims[1], 10), np.ones(10)*0, np.ones(10)*3, alpha=0.5, color='green', label='$T_3-T_0 < 3h$')
        if ylims[1] > 3.:
            ax.fill_between(np.linspace(xlims[0], 3, 10), np.ones(10)*3, np.ones(10)*ylims[1], alpha=0.5, color='blue', label='$T_2-T_1 < 3h$')
            if xlims[1] > 3.:
                ax.fill_between(np.linspace(3, xlims[1], 10), np.ones(10)*3, np.ones(10)*ylims[1], alpha=0.5, color='red', label='$T_2-T_1 > 3h$')
        
        ax.set_axisbelow(True)
        ax.grid(True)
        for item in (ax.get_xticklabels() + ax.get_yticklabels()):
            item.set_fontsize(16)
        ax.legend(fontsize=15, loc='upper right', fancybox=True, shadow=True)
        ax.set_xlabel('$T_2-T_1$ (hours)', size=18)
        ax.set_ylabel('$T_3-T_0$ (hours)', size=18)
        ax.set_title('HR-S&I turnaround monitoring between %s and %s'%(start_date.strftime('%d/%m/%Y'), end_date.strftime('%d/%m/%Y')), size=20, y=1.01)
        if output_figure.split('.')[-1] in ['eps', 'pdf']:
            fig.savefig(output_figure, bbox_inches='tight')
        else:
            fig.savefig(output_figure, bbox_inches='tight', dpi=100)
        print(output_figure)
        fig.clf()
        plt.close()
    except:
        fig.clf()
        plt.close()
        error = sys.exc_info()
        error = '%s\n%s'%(error[1], '\n'.join(traceback.format_tb(error[2])))
        raise Exception(error)
        
        
        
        
    #histogram
    output_figure = output_figure.split('.')[0] + '_histo.' + output_figure.split('.')[1]
        
    fig = plt.figure(figsize=(14, 8))
    try:
        ax = fig.add_subplot(1,1,1)
        dt = 5
        max_minute = max(dt*np.ceil(max(timeliness_vs_copernicus)*60./(1.*dt)), 60.*3)
        ax.hist(timeliness_vs_copernicus, bins=np.arange(0, max_minute+dt, dt)/60., facecolor='blue', edgecolor='white', label='%d valid (non-cloudy) products / %d L1Cs'%(njobs_success, njobs))
        
        xlims = list(ax.get_xlim())
        xlims[0] = 0.
        if xlims[1] < 5.:
            xlims[1] = 5.
        ax.set_xlim(xlims)
        ylims = list(ax.get_ylim())
        ylims[0] = 0.
        ax.plot(np.ones(10)*3, np.linspace(ylims[0], ylims[1], 10), color='red', lw=3, ls='--')
        ax.text(3., 0.5*(ylims[0]+ylims[1]), 'turnaround KPI', color='red', fontsize=20, fontweight='bold', rotation=90, ha="center", va="center", bbox=dict(facecolor='white', edgecolor='white'))
        ax.set_ylim([ylims[0], 1.1*ylims[1]])
        
        ax.set_axisbelow(True)
        ax.grid(True)
        for item in (ax.get_xticklabels() + ax.get_yticklabels()):
            item.set_fontsize(16)
        ax.legend(fontsize=15, loc='upper right', fancybox=True, shadow=True)
        ax.set_xlabel('Turnaround vs L1C ESA publication date (hours)', size=18)
        ax.set_ylabel('Number of products', size=18)
        ax.set_title('HR-S&I turnaround monitoring between %s and %s'%(start_date.strftime('%d/%m/%Y'), end_date.strftime('%d/%m/%Y')), size=20, y=1.01)
        if output_figure.split('.')[-1] in ['eps', 'pdf']:
            fig.savefig(output_figure, bbox_inches='tight')
        else:
            fig.savefig(output_figure, bbox_inches='tight', dpi=100)
        print(output_figure)
        fig.clf()
        plt.close()
    except:
        fig.clf()
        plt.close()
        error = sys.exc_info()
        error = '%s\n%s'%(error[1], '\n'.join(traceback.format_tb(error[2])))
        raise Exception(error)
        
        
if __name__ == '__main__':
    

    import argparse
    parser = argparse.ArgumentParser(description='plot timeliness')
    parser.add_argument("--output_figure", type=str, required=True, help='output_figure path')
    parser.add_argument("--db_adress", type=str, required=True, help='db_adress')
    parser.add_argument("--start_date", type=str, help='start date in %Y-%m-%d format, last day by default')
    parser.add_argument("--end_date", type=str, help='end date in %Y-%m-%d format, last day by default')
    args = parser.parse_args()
        
    if args.start_date is not None:
        args.start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
    if args.end_date is not None:
        args.end_date = datetime.strptime(args.end_date, '%Y-%m-%d')

    plot_timeliness(args.output_figure, start_date=args.start_date, end_date=args.end_date, db_adress=args.db_adress)

    
    
    
    
    
    
