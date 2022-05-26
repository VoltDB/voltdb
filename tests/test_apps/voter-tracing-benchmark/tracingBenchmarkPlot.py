# This file is part of VoltDB.
# Copyright (C) 2008-2022 Volt Active Data Inc.
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#

import matplotlib.pyplot as plt
import numpy as np


# visualize the statistical results produced by TracingBenchmark.java
# plot the statistical data in figure

def main():
    throughput = [[], []]
    avgLatency = [[], []]
    _99Latency = [[], []]
    maxLatency = [[], []]

    num = [0, 1]
    filenames = ["resTracingOff.txt", "resTracingOn.txt"]
    for i, filename in zip(num, filenames):
        if i == 0:
            print("{}, tracing off".format(filename))
        elif i == 1:
            print("{}, tracing on".format(filename))

        with open(filename) as f:
            for line in f:
                line.strip().rstrip()
                data = line.split()
                print(data)
                throughput[i].append(int(data[0]))
                avgLatency[i].append(float(data[1]))
                _99Latency[i].append(float(data[2]))
                maxLatency[i].append(float(data[3]))

    n = len(throughput[0]);
    num = [i + 1 for i in range(n)]

    fig, axs = plt.subplots(2, 2)
    axs[0, 0].set_title('Throughput')
    axs[0, 1].set_title('Avgerage Latency')
    axs[1, 0].set_title('99% Latency')
    axs[1, 1].set_title('Maximum Latency')

    axs[0, 0].set_ylabel('Throughput (txns/sec)')
    axs[0, 1].set_ylabel('Latency (millisec)')
    axs[1, 0].set_ylabel('Latency (millisec)')
    axs[1, 1].set_ylabel('Latency (millisec)')

    for i in [0, 1]:
        mark = "tracing on" if i == 1 else "tracing off"
        axs[0, 0].plot(num, throughput[i], label = mark)
        axs[0, 1].plot(num, avgLatency[i], label = mark)
        axs[1, 0].plot(num, _99Latency[i], label = mark)
        axs[1, 1].plot(num, maxLatency[i], label = mark)

    plt.legend()
    plt.show()
    fig.savefig("Tracing_benchmark_comparison_1.png")
    plt.close()


    throughput_diff = [y - x for x, y in zip(throughput[0], throughput[1])]
    avgLatency_diff = [y - x for x, y in zip(avgLatency[0], avgLatency[1])]
    _99Latency_diff = [y - x for x, y in zip(_99Latency[0], _99Latency[1])]
    maxLatency_diff = [y - x for x, y in zip(maxLatency[0], maxLatency[1])]

    fig2, axs2 = plt.subplots(2, 2)
    axs2[0, 0].set_title('Throughput')
    axs2[0, 1].set_title('Avgerage Latency')
    axs2[1, 0].set_title('99% Latency')
    axs2[1, 1].set_title('Maximum Latency')

    axs2[0, 0].set_ylabel('Relative increase (%)')
    axs2[0, 1].set_ylabel('Relative increase (%)')
    axs2[1, 0].set_ylabel('Relative increase (%)')
    axs2[1, 1].set_ylabel('Relative increase (%)')

    throughput_err = [100 * throughput_diff[i] / throughput[0][i] for i in range(n)];
    avgLatency_err = [100 * avgLatency_diff[i] / avgLatency[0][i] for i in range(n)];
    _99Latency_err = [100 * _99Latency_diff[i] / _99Latency[0][i] for i in range(n)];
    maxLatency_err = [100 * maxLatency_diff[i] / maxLatency[0][i] for i in range(n)];

    avg_throughput_err = 1.0 * sum(throughput_err[5:]) / (n - 5);
    avg_avgLatency_err = 1.0 * sum(avgLatency_err[5:]) / (n - 5);
    print("Avgerage percentage increase: throughput = {0:.2f}%, avgLatency = {1:.2f}%".format(avg_throughput_err, avg_avgLatency_err))

    axs2[0, 0].plot(num, throughput_err)
    axs2[0, 1].plot(num, avgLatency_err)
    axs2[1, 0].plot(num, _99Latency_err)
    axs2[1, 1].plot(num, maxLatency_err)

    plt.legend()
    plt.show()
    fig2.savefig("Tracing_benchmark_relative_change_2.png")
    plt.close()


if __name__ == "__main__":
    main()
