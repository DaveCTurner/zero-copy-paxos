#!/usr/bin/python

import matplotlib        as mpl
import matplotlib.cm     as cm
import matplotlib.pyplot as plt
import numpy             as np
import os
import re

print "Reading node stats"

stats_lines = []

stats_line_re = re.compile(r"^stats: real +(?P<real_time>[0-9]+)us user +(?P<user_time>[0-9]+)us sys +(?P<sys_time>[0-9]+)us active slots \[ *(?P<slot>[0-9]+),.*")
startup_line_re = re.compile(r"^Starting as cluster +(?P<cluster_id>[0-9a-f-]+) node +(?P<node_id>[0-9]+)")

slots_by_client_time = []
cpu_times_by_slot = {}

for stats_file_name in ['logs/node-1-10.20.30.68-1505375940.log',
                        'logs/node-1-10.20.30.68-1505383359.log',
                        'logs/node-2-10.20.30.74-1505375943.log',
                        'logs/node-2-10.20.30.74-1505383380.log']:
  cluster_id       = None
  node_id          = None
  current_run_data = None

  with open(stats_file_name, 'r') as stats_file:
    for line in stats_file:

      startup_match = startup_line_re.match(line)
      if startup_match is not None:
        cluster_id, node_id = startup_match.groups()
        node_id = int(node_id)
        if cluster_id not in cpu_times_by_slot:
          cpu_times_by_slot[cluster_id] = {}
        current_run_data = []
        cpu_times_by_slot[cluster_id][node_id] = current_run_data

      m = stats_line_re.match(line)
      if m is not None:
        ela, usr, sys, slot = m.groups()
        current_run_data.append((int(slot), int(usr), int(sys)))
        if node_id == 1:
          slots_by_client_time.append((int(ela), cluster_id, int(slot)))

slots_by_client_time.sort()

def get_stats_line(stats_lines, target):
  if target <= stats_lines[0][0]:
    return stats_lines[0]

  lo = 0
  hi = len(stats_lines)

  if stats_lines[hi-1][0] < target:
    print "ARGH target", target, "out of range - using ", stats_lines[hi-1][0]
    return stats_lines[hi-1]

  # invariant: stats_lines[lo] < target <= stats_lines[hi]

  while (lo+1 < hi):
    mid = lo + (hi - lo) / 2 # lo < mid < hi
    mid_val = stats_lines[mid][0]
    if target <= mid_val:
      hi = mid
    else:
      lo = mid

  return stats_lines[hi]

print "Reading log files"

client_log_files = []

for root, dirs, files in os.walk('logs/'):
  client_log_files = client_log_files + [ root + f for f in files if f.startswith('client-') ]
  break

results = []

for f in client_log_files:
  with open(f, 'r') as log_file:
    results_line = None
    for line in log_file:
      cells = line.split()
      header = cells[0]
      if cells[0] == 'results:':
        results_line = cells[1:]
      elif cells[0] == 'latency':
        t_start = float(results_line[2])
        t_end   = float(results_line[3])

        slot_start = get_stats_line(slots_by_client_time, t_start * 1000000)
        slot_end   = get_stats_line(slots_by_client_time, t_end   * 1000000)
        cluster_id = slot_start[1]
        if cluster_id != slot_end[1]:
          print "ARGH run crosses clusters"

        # Look up leader line by clock time
        ldr_start = get_stats_line(cpu_times_by_slot[cluster_id][1], slot_start[2])
        ldr_end   = get_stats_line(cpu_times_by_slot[cluster_id][1], slot_end[2])
        # Look up follower stats by slot
        fol_start = get_stats_line(cpu_times_by_slot[cluster_id][2], slot_start[2])
        fol_end   = get_stats_line(cpu_times_by_slot[cluster_id][2], slot_end[2])

        results.append({ 'rate_MBps': int(float(results_line[0])) / 1000000,
                         'size_B':          int(results_line[1]),
                         'tstart':        t_start,
                         'tend':          t_end,
                         'ela_ms':          int(results_line[4]),
                         'usr_ms':          int(results_line[5]),
                         'sys_ms':          int(results_line[6]),
                         'acks':            int(results_line[7]),
                         'acked_B':         int(results_line[8]),
                         'ldr_usr_ms': (ldr_end[1] - ldr_start[1]) / 1000,
                         'ldr_sys_ms': (ldr_end[2] - ldr_start[2]) / 1000,
                         'fol_usr_ms': (fol_end[1] - fol_start[1]) / 1000,
                         'fol_sys_ms': (fol_end[2] - fol_start[2]) / 1000,
                         'latencies': sorted([ float(l) for l in cells[2:]]) })

all_sizes = set()
all_rates = set()

for r in results:
  all_sizes.add(r['size_B'])
  all_rates.add(r['rate_MBps'])

colormap = cm.ScalarMappable(norm=mpl.colors.Normalize(vmin=0, vmax=len(all_sizes)-1),
                             cmap=cm.get_cmap('plasma'))

for size_index, size in enumerate(sorted(all_sizes)):
  colour = colormap.to_rgba(size_index)
  for r in results:
    if r['size_B'] == size:
      r['colour'] = colour
      r['size_index'] = size_index

for rate_index, rate in enumerate(sorted(all_rates)):
  for r in results:
    if r['rate_MBps'] == rate:
      r['rate_index'] = rate_index

results_grid = [ [None for s in range(len(all_sizes))]
                       for r in range(len(all_rates))]
for r in results:
  results_grid[r['rate_index']][r['size_index']] = r

# CPU usage

## Chart

print('CPU usage')

fig, ax_list = plt.subplots(2,2,
                            sharex='col', sharey='row',
                            figsize=(8,3), dpi=80)
plt.xlabel('Target bandwidth (B/s)')

for ax in ax_list.ravel():
  ax.spines['top'].set_visible(False)
  ax.spines['right'].set_visible(False)
  ax.spines['bottom'].set_visible(False)
  ax.spines['left'].set_visible(False)
  ax.semilogx(basex=10)
  ax.set_xlim([5e6,250e6])
  ax.set_ylim([0,100])
  ax.yaxis.get_major_locator().set_params(nbins=3)

ax_list[0,0].set_title('User')
ax_list[0,1].set_title('System')
ax_list[0,0].set_ylabel('Leader CPU %')
ax_list[1,0].set_ylabel('Follower CPU %')

fig.tight_layout(pad=1.5, h_pad=2.0)

for   col_index, col in enumerate(['usr', 'sys']):
  for row_index, row in enumerate(['ldr', 'fol']):

    ax = ax_list[row_index, col_index]
    sel = row + '_' + col + '_ms'

    for size_index, size in enumerate(sorted(all_sizes)):
      colour = colormap.to_rgba(size_index)

      xys = sorted([[r['rate_MBps'] * 1000000.0,
                     100.0 * float(r[sel]) / r['ela_ms']]
                        for r in results if r['size_B'] == size])
      xs = [xy[0] for xy in xys]
      ys = [xy[1] for xy in xys]

      ax.plot(xs, ys, label=(str(size) + ' B'),
                       color=colour, linewidth=3.0)

plt.savefig('cpu-usage.png')
plt.close()

# Write rate by target bandwidth
print "Write rate by target bandwidth"

## Chart

plt.figure(figsize=(8,4), dpi=80)
plt.loglog(basex=10, basey=10)
plt.xlabel('Target bandwidth (B/s)')
plt.ylabel('Write rate (Hz)')
plt.tight_layout(pad=1.5, h_pad=2.0)
ax = plt.gca()
ax.set_xlim([5e6,250e6])
ax.set_ylim([5e3,2.5e6])
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
ax.spines['bottom'].set_visible(False)
ax.spines['left'].set_visible(False)

for size_index, size in enumerate(sorted(all_sizes)):
  colour = colormap.to_rgba(size_index)

  xys = sorted([[r['rate_MBps'] * 1000000.0,
                 r['acked_B'] * 1000.0 / r['ela_ms'] / r['size_B']]
                    for r in results if r['size_B'] == size])
  xs = [xy[0] for xy in xys]
  ys = [xy[1] for xy in xys]

  plt.plot(xs, ys, label=(str(size) + ' B'),
                   color=colour, linewidth=3.0)

plt.legend(title='Write size')

plt.axvspan(175e6, 250e6, facecolor='#c0c0c0', alpha=0.4)
plt.axhspan(1e6, 2.5e6, facecolor='#c0c0c0', alpha=0.4)

plt.savefig('transaction-rate.png')
plt.close()

## Table

with open('transaction-rate.txt', 'w') as table:
  table.write('|{0:10s}'.format(''));
  for size in sorted(all_sizes):
    table.write('| {0:4d} B'.format(size))
  table.write('\n')

  table.write('|----------')
  for size in sorted(all_sizes):
    table.write('|-------'.format(size))
  table.write('\n')

  for rate_index, rate in enumerate(sorted(all_rates)):
    table.write('|{0:3d} MB/s  '.format(rate))
    for size_index, size in enumerate(sorted(all_sizes)):
      r = results_grid[rate_index][size_index]
      if r is None:
        table.write('|       ')
      else:
        result = r['acked_B'] / 1000.0 / r['ela_ms'] / r['size_B']
        table.write('|{0:7.3f}'.format(result))
    table.write('\n')

# Fraction of target bandwidth delivered
print('Fraction of target bandwidth delivered')

## Chart

plt.figure(figsize=(8,4), dpi=80)
plt.semilogx(basex=10)
plt.xlabel('Target bandwidth (B/s)')
plt.ylabel('% of target delivered')
plt.tight_layout(pad=1.5, h_pad=2.0)
ax = plt.gca()
ax.set_xlim([5e6,250e6])
ax.set_ylim([0,101])
ax.yaxis.get_major_locator().set_params(nbins=3)
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
ax.spines['bottom'].set_visible(False)
ax.spines['left'].set_visible(False)

for size_index, size in enumerate(sorted(all_sizes)):
  colour = colormap.to_rgba(size_index)

  xys = sorted([[r['rate_MBps'] * 1000000.0,
                 r['acked_B'] / 10.0 / r['ela_ms'] / r['rate_MBps']]
                    for r in results if r['size_B'] == size])
  xs = [xy[0] for xy in xys]
  ys = [xy[1] for xy in xys]

  plt.plot(xs, ys, label=(str(size) + ' B'),
                   color=colour, linewidth=3.0)

plt.legend(title='Write size')

plt.savefig('data-rate.png')
plt.close()

## Table

with open('data-rate.txt', 'w') as table:
  table.write('|{0:10s}'.format(''));
  for size in sorted(all_sizes):
    table.write('| {0:4d} B'.format(size))
  table.write('\n')

  table.write('|----------')
  for size in sorted(all_sizes):
    table.write('|-------'.format(size))
  table.write('\n')

  for rate_index, rate in enumerate(sorted(all_rates)):
    table.write('|{0:3d} MB/s  '.format(rate))
    for size_index, size in enumerate(sorted(all_sizes)):
      r = results_grid[rate_index][size_index]
      if r is None:
        table.write('|       ')
      else:
        result = r['acked_B'] / 1000.0 / r['ela_ms'] / r['rate_MBps']
        table.write('|{0:7.3f}'.format(result))
    table.write('\n')

# Batch size by target bandwidth
print('Batch size by target bandwidth')

## Chart

plt.figure(figsize=(8,4), dpi=80)
plt.loglog(basex=10, basey=10)
plt.xlabel('Target bandwidth (B/s)')
plt.ylabel('Mean batch size (B)')
plt.tight_layout(pad=1.5, h_pad=2.0)
ax = plt.gca()
ax.set_xlim([5e6,250e6])
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
ax.spines['bottom'].set_visible(False)
ax.spines['left'].set_visible(False)

for size_index, size in enumerate(sorted(all_sizes)):
  colour = colormap.to_rgba(size_index)

  xys = sorted([[r['rate_MBps'] * 1000000.0,
                 r['acked_B'] / r['acks']]
                    for r in results if r['size_B'] == size])
  xs = [xy[0] for xy in xys]
  ys = [xy[1] for xy in xys]

  plt.plot(xs, ys, label=(str(size) + ' B'),
                   color=colour, linewidth=3.0)

plt.legend(title='Write size')

plt.savefig('mean-ack-size.png')
plt.close()

## Table

with open('mean-ack-size.txt', 'w') as table:
  table.write('|{0:10s}'.format(''));
  for size in sorted(all_sizes):
    table.write('|   {0:4d} B'.format(size))
  table.write('\n')

  table.write('|----------')
  for size in sorted(all_sizes):
    table.write('|---------'.format(size))
  table.write('\n')

  for rate_index, rate in enumerate(sorted(all_rates)):
    table.write('|{0:3d} MB/s  '.format(rate))
    for size_index, size in enumerate(sorted(all_sizes)):
      r = results_grid[rate_index][size_index]
      if r is None:
        table.write('|         ')
      else:
        result = r['acked_B'] / r['acks']
        table.write('|{0:9d}'.format(result))
    table.write('\n')

# Latency

def latency(y, distribution):
  seg_count = len(distribution) - 1
  i = int(y * seg_count)
  if (i < 0): return distribution[0]
  if (i >= seg_count): return distribution[seg_count]
  x0 = distribution[i]
  x1 = distribution[i+1]
  y0 = float(i)   / seg_count
  y1 = float(i+1) / seg_count
  result = x0 + (x1 - x0) * float(y - y0) / float(y1 - y0)
  if result < 0:
    print x0, x1, y0, y1, seg_count, result
  return result

print('Latency distributions')

plot_cols = 4
plot_rows = (len(all_rates)+plot_cols-1)/plot_cols

fig, ax_list = plt.subplots(plot_rows, plot_cols,
                            sharex='col', sharey='row',
                            figsize=(8,7), dpi=80)

for ax_row in range(plot_rows):
  ax = ax_list[ax_row, 0]
  ax.set_ylabel('P(exceedance)')

for ax_col in range(plot_cols):
  ax = ax_list[plot_rows-1, ax_col]
  ax.set_xlabel('Latency (s)')

for ax in ax_list.ravel():
  ax.spines['top'].set_visible(False)
  ax.spines['right'].set_visible(False)
  ax.spines['bottom'].set_visible(False)
  ax.spines['left'].set_visible(False)
  ax.set_yticks([])
  ax.loglog(basex=10,basey=10)
  ax.set_xlim([5e-4,5e-1])
  ax.set_ylim([1e-3,1.1])

fig.tight_layout(pad=1.5, h_pad=2.0)

cumulative_points = 2000
for rate_index, rate in enumerate(sorted(all_rates)):
  ax_row = rate_index % plot_rows
  ax_col = rate_index / plot_rows
  ax = ax_list[ax_row, ax_col]

  ax.set_title(str(rate) + ' MB/s')

  for size_index in range(len(all_sizes)):
    for r in results:
      if r['rate_index'] != rate_index:
        continue
      if r['size_index'] != size_index:
        continue

      ys = [i / float(cumulative_points) for i in range(cumulative_points+1)]
      xs = [latency(y, r['latencies']) for y in ys]
      cys = [1.0 - y for y in ys]

      ax.plot(xs, cys, label=(str(r['size_B']) + ' B'),
                       color=r['colour'])

plt.savefig('latency-distributions.png')
plt.close()

print('Latency percentiles')

percentiles = [50, 90, 95, 99, 99.5, 99.9]

for idx, percentile in enumerate(percentiles):

  with open('latency-' + str(percentile) + '-percentile.txt', 'w') as table:
    table.write('|{0:10s}'.format(''));
    for size in sorted(all_sizes):
      table.write('|   {0:4d} B'.format(size))
    table.write('\n')

    table.write('|----------')
    for size in sorted(all_sizes):
      table.write('|---------'.format(size))
    table.write('\n')

    for rate_index, rate in enumerate(sorted(all_rates)):
      table.write('|{0:3d} MB/s  '.format(rate))
      for size_index, size in enumerate(sorted(all_sizes)):
        r = results_grid[rate_index][size_index]
        if r is None:
          table.write('|         ')
        else:
          result = latency(percentile/100.0, r['latencies']) * 1000.0
          table.write('|{0:9.3f}'.format(result))
      table.write('\n')
